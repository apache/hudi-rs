// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! What a slice read pushes into the parquet reader from the caller's filters.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, DataType, Schema};
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
use parquet::arrow::{ProjectionMask, parquet_to_arrow_schema};
use parquet::file::metadata::ParquetMetaData;

use crate::expr::filter::{Filter, filters_to_row_mask};
use crate::metadata::meta_field::MetaField;
use crate::statistics::StatisticsContainer;
use crate::storage::{RowFilterBuilder, RowGroupSelector};
use crate::table::file_pruner::FilePruner;

/// The caller's filters compiled for the parquet reader: a row-group selector
/// over the footer statistics and a row filter that decodes the filter
/// columns first and the rest only for the rows that pass.
///
/// Both are conservative. A file without one of the filter columns gets no
/// row filter, and a row group without statistics is kept; the row-level mask
/// the reader applies afterwards still decides every row.
pub(crate) struct FilterPushdown {
    pub row_filter: RowFilterBuilder,
    pub row_groups: RowGroupSelector,
    /// Every filter is on a record key field, which no log record can change,
    /// so the filter may drop base rows before a merge-on-read merge. Always
    /// true for a slice that merges, which keeps only those filters.
    pub mor_pk_safe: bool,
}

/// `table_schema` is the schema the read returns rows in. A file written
/// before a type widening stores a filter column in a narrower type, and a
/// literal compared in that type can disagree with the same comparison in
/// the table's type, so such a file gets nothing pushed.
///
/// A slice that `merges` log records into its base file pushes only the
/// record key filters: a filter on any other column could drop a base row
/// that a log record would have lost to.
pub(crate) fn filter_pushdown(
    filters: &[Filter],
    record_key_fields: &[String],
    table_schema: &Schema,
    merges: bool,
) -> Option<FilterPushdown> {
    let is_key = |filter: &&Filter| {
        filter.field == MetaField::RecordKey.as_ref() || record_key_fields.contains(&filter.field)
    };
    let filters: Vec<Filter> = if merges {
        filters.iter().filter(is_key).cloned().collect()
    } else {
        filters.to_vec()
    };
    if filters.is_empty() {
        return None;
    }
    let mor_pk_safe = filters.iter().all(|filter| is_key(&filter));
    let table_types: Vec<(String, DataType)> = filters
        .iter()
        .map(|filter| {
            let data_type = table_schema
                .field_with_name(&filter.field)
                .map(|f| f.data_type().clone())
                .unwrap_or(DataType::Null);
            (filter.field.clone(), data_type)
        })
        .collect();
    // Every filter column, stored in the file as the table has it.
    let same_types = |file_schema: &Schema, table_types: &[(String, DataType)]| {
        table_types.iter().all(|(name, data_type)| {
            file_schema
                .field_with_name(name)
                .is_ok_and(|f| f.data_type() == data_type)
        })
    };
    let for_rows = filters.clone();
    let row_types = table_types.clone();
    // Checked against the schema the reader decodes with (its embedded Arrow
    // hint included), which is what the predicate sees.
    let row_filter: RowFilterBuilder = Arc::new(move |parquet_schema, read_schema| {
        if !same_types(read_schema, &row_types) {
            return None;
        }
        let root = parquet_schema.root_schema();
        let mut columns: Vec<usize> = for_rows
            .iter()
            .map(|filter| {
                root.get_fields()
                    .iter()
                    .position(|field| field.name() == filter.field)
            })
            .collect::<Option<_>>()?;
        columns.sort_unstable();
        columns.dedup();
        let mask = ProjectionMask::roots(parquet_schema, columns);
        let filters = for_rows.clone();
        let predicate = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
            filters_to_row_mask(&filters, &batch)
                .map_err(|e| ArrowError::ComputeError(e.to_string()))
        });
        Some(RowFilter::new(vec![Box::new(predicate)]))
    });
    // Statistics leave NaN out of a float column's range, so a range check on
    // one could drop a row group holding NaN; those columns are left to the
    // row filter.
    let for_groups: Vec<Filter> = filters
        .iter()
        .filter(|filter| {
            !matches!(
                table_schema
                    .field_with_name(&filter.field)
                    .map(|f| f.data_type()),
                Ok(DataType::Float16 | DataType::Float32 | DataType::Float64)
            )
        })
        .cloned()
        .collect();
    let group_types = table_types;
    let row_groups: RowGroupSelector = Arc::new(move |metadata: &ParquetMetaData| {
        let file = metadata.file_metadata();
        let schema =
            parquet_to_arrow_schema(file.schema_descr(), file.key_value_metadata()).ok()?;
        if !same_types(&schema, &group_types) {
            return None;
        }
        let pruner = FilePruner::new(&for_groups, &schema, &Schema::empty()).ok()?;
        Some(
            (0..metadata.num_row_groups())
                .filter(|&index| {
                    pruner.should_include(&StatisticsContainer::from_row_group(
                        metadata.row_group(index),
                        &schema,
                    ))
                })
                .collect(),
        )
    });
    Some(FilterPushdown {
        row_filter,
        row_groups,
        mor_pk_safe,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::filter::col;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ])
    }

    /// Ten rows `k00..k09` with values `0..9`, two rows per row group.
    fn parquet_file() -> Bytes {
        let schema = Arc::new(schema());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    (0..10).map(|i| format!("k{i:02}")).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from_iter_values(0..10)),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(out)
    }

    fn ids_through(pushdown: &FilterPushdown, file: Bytes) -> (Vec<usize>, Vec<String>) {
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let groups = (pushdown.row_groups)(builder.metadata()).unwrap();
        let filter = (pushdown.row_filter)(builder.parquet_schema(), builder.schema()).unwrap();
        let reader = builder
            .with_row_groups(groups.clone())
            .with_row_filter(filter)
            .build()
            .unwrap();
        let ids = reader
            .map(|b| b.unwrap())
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap().to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        (groups, ids)
    }

    #[test]
    fn test_pushdown_selects_row_groups_and_rows() {
        let file = parquet_file();
        let one = filter_pushdown(
            &[col("id").eq("k05")],
            &["id".to_string()],
            &schema(),
            false,
        )
        .unwrap();
        assert!(one.mor_pk_safe);
        assert_eq!(
            ids_through(&one, file.clone()),
            (vec![2], vec!["k05".to_string()])
        );

        let two =
            filter_pushdown(&[col("id").in_list(["k01", "k08"])], &[], &schema(), false).unwrap();
        assert!(!two.mor_pk_safe);
        assert_eq!(
            ids_through(&two, file.clone()),
            (vec![0, 4], vec!["k01".to_string(), "k08".to_string()])
        );

        let both = filter_pushdown(
            &[col("v").gte("6"), col("id").ne("k07")],
            &[],
            &schema(),
            false,
        )
        .unwrap();
        assert_eq!(
            ids_through(&both, file.clone()),
            (
                vec![3, 4],
                vec!["k06".to_string(), "k08".to_string(), "k09".to_string()]
            )
        );

        let none = filter_pushdown(&[col("v").gt("100")], &[], &schema(), false).unwrap();
        assert_eq!(ids_through(&none, file), (vec![], vec![]));
    }

    /// A slice that merges keeps only the record key half of a mixed filter.
    #[test]
    fn test_a_merging_slice_pushes_only_record_key_filters() {
        let file = parquet_file();
        let keys = ["id".to_string()];
        let mixed = [col("id").in_list(["k01", "k08"]), col("v").eq("8")];

        let merging = filter_pushdown(&mixed, &keys, &schema(), true).unwrap();
        assert!(merging.mor_pk_safe);
        assert_eq!(
            ids_through(&merging, file.clone()),
            (vec![0, 4], vec!["k01".to_string(), "k08".to_string()])
        );
        let plain = filter_pushdown(&mixed, &keys, &schema(), false).unwrap();
        assert!(!plain.mor_pk_safe);
        assert_eq!(
            ids_through(&plain, file),
            (vec![4], vec!["k08".to_string()])
        );
        assert!(filter_pushdown(&[col("v").eq("8")], &keys, &schema(), true).is_none());
    }

    /// A file holding a filter column in another type than the table (a file
    /// from before a widening) gets nothing pushed, and a float column gets no
    /// row-group pruning.
    #[test]
    fn test_pushdown_skips_a_narrower_file_type_and_float_statistics() {
        let file = parquet_file();
        let wide = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Decimal128(20, 0), false),
        ]);
        let pushdown = filter_pushdown(&[col("v").gt("5")], &[], &wide, false).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file.clone()).unwrap();
        assert!((pushdown.row_filter)(builder.parquet_schema(), builder.schema()).is_none());
        assert!((pushdown.row_groups)(builder.metadata()).is_none());

        let floats = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Float64, false),
        ]);
        let pushdown = filter_pushdown(
            &[col("v").gt("5"), col("id").eq("k01")],
            &[],
            &floats,
            false,
        )
        .unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        // The file stores `v` as Int64, so the type check skips the file; with
        // a Float64 file the selector would still ignore the `v` filter.
        assert!((pushdown.row_groups)(builder.metadata()).is_none());
    }

    /// A column the file does not have gets no row filter and no row-group
    /// pruning; the reader's mask still handles it.
    #[test]
    fn test_pushdown_skips_a_column_the_file_lacks() {
        let file = parquet_file();
        let pushdown = filter_pushdown(&[col("missing").eq("x")], &[], &schema(), false).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert!((pushdown.row_filter)(builder.parquet_schema(), builder.schema()).is_none());
        // No opinion on row groups either: the reader fetches them all.
        assert!((pushdown.row_groups)(builder.metadata()).is_none());
        assert!(filter_pushdown(&[], &[], &schema(), false).is_none());
    }
}
