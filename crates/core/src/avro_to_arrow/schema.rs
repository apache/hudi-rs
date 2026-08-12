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

use crate::error::{CoreError, Result};
use apache_avro::Schema as AvroSchema;
use apache_avro::schema::{Alias, DecimalSchema, EnumSchema, FixedSchema, Name, RecordSchema};
use apache_avro::types::Value;
use arrow::datatypes::{DataType, IntervalUnit, Schema, TimeUnit, UnionMode};
use arrow::datatypes::{Field, UnionFields};
use parquet::variant::VariantType;
use std::collections::HashMap;
use std::sync::Arc;

const AVRO_LOGICAL_TYPE_KEY: &str = "logicalType";
const HUDI_VARIANT_LOGICAL_TYPE: &str = "variant";

/// Converts an avro schema to an arrow schema
pub fn to_arrow_schema(avro_schema: &apache_avro::Schema) -> Result<Schema> {
    let mut schema_fields = vec![];
    match avro_schema {
        AvroSchema::Record(RecordSchema { fields, .. }) => {
            for field in fields {
                schema_fields.push(schema_to_field_with_props(
                    &field.schema,
                    Some(&field.name),
                    field.is_nullable(),
                    Some(external_props(&field.schema)),
                )?)
            }
        }
        schema => schema_fields.push(schema_to_field(schema, Some(""), false)?),
    }

    let schema = Schema::new(schema_fields);
    Ok(schema)
}

fn schema_to_field(
    schema: &apache_avro::Schema,
    name: Option<&str>,
    nullable: bool,
) -> Result<Field> {
    schema_to_field_with_props(schema, name, nullable, Default::default())
}

fn schema_to_field_with_props(
    schema: &AvroSchema,
    name: Option<&str>,
    nullable: bool,
    props: Option<HashMap<String, String>>,
) -> Result<Field> {
    let mut nullable = nullable;
    let field_type: DataType = match schema {
        AvroSchema::Ref { .. } => todo!("Add support for AvroSchema::Ref"),
        AvroSchema::Null => DataType::Null,
        AvroSchema::Boolean => DataType::Boolean,
        AvroSchema::Int => DataType::Int32,
        AvroSchema::Long => DataType::Int64,
        AvroSchema::Float => DataType::Float32,
        AvroSchema::Double => DataType::Float64,
        AvroSchema::Bytes => DataType::Binary,
        AvroSchema::String => DataType::Utf8,
        AvroSchema::Array(item_schema) => DataType::List(Arc::new(schema_to_field_with_props(
            &item_schema.items,
            Some("element"),
            false,
            None,
        )?)),
        AvroSchema::Map(value_schema) => {
            let value_field =
                schema_to_field_with_props(&value_schema.types, Some("value"), false, None)?;
            DataType::Dictionary(
                Box::new(DataType::Utf8),
                Box::new(value_field.data_type().clone()),
            )
        }
        AvroSchema::Union(us) => {
            // If there are only two variants and one of them is null, set the other type as the field data type
            let has_nullable = us
                .find_schema_with_known_schemata::<apache_avro::Schema>(&Value::Null, None, &None)
                .is_some();
            let sub_schemas = us.variants();
            if has_nullable && sub_schemas.len() == 2 {
                nullable = true;
                if let Some(schema) = sub_schemas
                    .iter()
                    .find(|&schema| !matches!(schema, AvroSchema::Null))
                {
                    schema_to_field_with_props(schema, None, has_nullable, None)?
                        .data_type()
                        .clone()
                } else {
                    return Err(apache_avro::Error::new(
                        apache_avro::error::Details::GetUnionDuplicate,
                    )
                    .into());
                }
            } else {
                let fields = sub_schemas
                    .iter()
                    .map(|s| schema_to_field_with_props(s, None, has_nullable, None))
                    .collect::<Result<Vec<Field>>>()?;
                let type_ids = 0_i8..fields.len() as i8;
                DataType::Union(UnionFields::new(type_ids, fields), UnionMode::Dense)
            }
        }
        AvroSchema::Record(RecordSchema { fields, .. }) => {
            let fields: Result<_> = fields
                .iter()
                .map(|field| {
                    let mut props = HashMap::new();
                    if let Some(doc) = &field.doc {
                        props.insert("avro::doc".to_string(), doc.clone());
                    }
                    /*if let Some(aliases) = fields.aliases {
                        props.insert("aliases", aliases);
                    }*/
                    schema_to_field_with_props(
                        &field.schema,
                        Some(&field.name),
                        field.is_nullable(),
                        Some(props),
                    )
                })
                .collect();
            DataType::Struct(fields?)
        }
        AvroSchema::Enum(EnumSchema { .. }) => DataType::Utf8,
        AvroSchema::Fixed(FixedSchema { size, .. }) => DataType::FixedSizeBinary(*size as i32),
        AvroSchema::Decimal(DecimalSchema {
            precision, scale, ..
        }) => DataType::Decimal128(*precision as u8, *scale as i8),
        AvroSchema::BigDecimal => DataType::LargeBinary,
        AvroSchema::Uuid => DataType::FixedSizeBinary(16),
        AvroSchema::Date => DataType::Date32,
        AvroSchema::TimeMillis => DataType::Time32(TimeUnit::Millisecond),
        AvroSchema::TimeMicros => DataType::Time64(TimeUnit::Microsecond),
        AvroSchema::TimestampMillis => DataType::Timestamp(TimeUnit::Millisecond, None),
        AvroSchema::TimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, None),
        AvroSchema::TimestampNanos => DataType::Timestamp(TimeUnit::Nanosecond, None),
        AvroSchema::LocalTimestampMillis => todo!(),
        AvroSchema::LocalTimestampMicros => todo!(),
        AvroSchema::LocalTimestampNanos => todo!(),
        AvroSchema::Duration => DataType::Duration(TimeUnit::Millisecond),
    };

    let data_type = field_type.clone();
    let name = name.unwrap_or_else(|| default_field_name(&data_type));

    let mut field = Field::new(name, field_type, nullable);
    field.set_metadata(props.unwrap_or_default());
    if is_hudi_variant_schema(schema) {
        validate_hudi_variant_field(&field)?;
        field.try_with_extension_type(VariantType)?;
    }
    Ok(field)
}

fn is_hudi_variant_schema(schema: &AvroSchema) -> bool {
    match schema {
        AvroSchema::Record(record_schema) => is_hudi_variant_record(record_schema),
        AvroSchema::Union(union_schema) => {
            let variants = union_schema.variants();
            variants.len() == 2
                && variants
                    .iter()
                    .any(|schema| matches!(schema, AvroSchema::Null))
                && variants.iter().any(is_hudi_variant_schema)
        }
        _ => false,
    }
}

fn is_hudi_variant_record(record_schema: &RecordSchema) -> bool {
    matches!(
        record_schema.attributes.get(AVRO_LOGICAL_TYPE_KEY),
        Some(serde_json::Value::String(logical_type)) if logical_type == HUDI_VARIANT_LOGICAL_TYPE
    )
}

fn validate_hudi_variant_field(field: &Field) -> Result<()> {
    let DataType::Struct(fields) = field.data_type() else {
        return Err(CoreError::Schema(format!(
            "Hudi variant field '{}' must be an Avro record",
            field.name()
        )));
    };

    let metadata = fields.iter().find(|child| child.name() == "metadata");
    match metadata {
        Some(child) if is_variant_binary_field(child) => {}
        Some(child) => {
            return Err(CoreError::Schema(format!(
                "Hudi variant field '{}' must have binary 'metadata', got {}",
                field.name(),
                child.data_type()
            )));
        }
        None => {
            return Err(CoreError::Schema(format!(
                "Hudi variant field '{}' must contain a 'metadata' field",
                field.name()
            )));
        }
    }

    if let Some(value) = fields.iter().find(|child| child.name() == "value")
        && !is_variant_binary_field(value)
    {
        return Err(CoreError::Schema(format!(
            "Hudi variant field '{}' must have binary 'value', got {}",
            field.name(),
            value.data_type()
        )));
    }

    if !fields
        .iter()
        .any(|child| matches!(child.name().as_str(), "value" | "typed_value"))
    {
        return Err(CoreError::Schema(format!(
            "Hudi variant field '{}' must contain 'value' or 'typed_value'",
            field.name()
        )));
    }

    Ok(())
}

fn is_variant_binary_field(field: &Field) -> bool {
    matches!(
        field.data_type(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

fn default_field_name(dt: &DataType) -> &str {
    match dt {
        DataType::Null => "null",
        DataType::Boolean => "bit",
        DataType::Int8 => "tinyint",
        DataType::Int16 => "smallint",
        DataType::Int32 => "int",
        DataType::Int64 => "bigint",
        DataType::UInt8 => "uint1",
        DataType::UInt16 => "uint2",
        DataType::UInt32 => "uint4",
        DataType::UInt64 => "uint8",
        DataType::Float16 => "float2",
        DataType::Float32 => "float4",
        DataType::Float64 => "float8",
        DataType::Date32 => "dateday",
        DataType::Date64 => "datemilli",
        DataType::Time32(tu) | DataType::Time64(tu) => match tu {
            TimeUnit::Second => "timesec",
            TimeUnit::Millisecond => "timemilli",
            TimeUnit::Microsecond => "timemicro",
            TimeUnit::Nanosecond => "timenano",
        },
        DataType::Timestamp(tu, tz) => {
            if tz.is_some() {
                match tu {
                    TimeUnit::Second => "timestampsectz",
                    TimeUnit::Millisecond => "timestampmillitz",
                    TimeUnit::Microsecond => "timestampmicrotz",
                    TimeUnit::Nanosecond => "timestampnanotz",
                }
            } else {
                match tu {
                    TimeUnit::Second => "timestampsec",
                    TimeUnit::Millisecond => "timestampmilli",
                    TimeUnit::Microsecond => "timestampmicro",
                    TimeUnit::Nanosecond => "timestampnano",
                }
            }
        }
        DataType::Duration(_) => "duration",
        DataType::Interval(unit) => match unit {
            IntervalUnit::YearMonth => "intervalyear",
            IntervalUnit::DayTime => "intervalmonth",
            IntervalUnit::MonthDayNano => "intervalmonthdaynano",
        },
        DataType::Binary => "varbinary",
        DataType::FixedSizeBinary(_) => "fixedsizebinary",
        DataType::LargeBinary => "largevarbinary",
        DataType::Utf8 => "varchar",
        DataType::LargeUtf8 => "largevarchar",
        DataType::List(_) => "list",
        DataType::FixedSizeList(_, _) => "fixed_size_list",
        DataType::LargeList(_) => "largelist",
        DataType::Struct(_) => "struct",
        DataType::Union(_, _) => "union",
        DataType::Dictionary(_, _) => "map",
        DataType::Map(_, _) => unimplemented!("Map support not implemented"),
        DataType::RunEndEncoded(_, _) => {
            unimplemented!("RunEndEncoded support not implemented")
        }
        DataType::Utf8View
        | DataType::BinaryView
        | DataType::ListView(_)
        | DataType::LargeListView(_) => {
            unimplemented!("View support not implemented")
        }
        DataType::Decimal32(_, _) => "decimal",
        DataType::Decimal64(_, _) => "decimal",
        DataType::Decimal128(_, _) => "decimal",
        DataType::Decimal256(_, _) => "decimal",
    }
}

fn external_props(schema: &AvroSchema) -> HashMap<String, String> {
    let mut props = HashMap::new();
    match &schema {
        AvroSchema::Record(RecordSchema { doc: Some(doc), .. })
        | AvroSchema::Enum(EnumSchema { doc: Some(doc), .. })
        | AvroSchema::Fixed(FixedSchema { doc: Some(doc), .. }) => {
            props.insert("avro::doc".to_string(), doc.clone());
        }
        _ => {}
    }
    match &schema {
        AvroSchema::Record(RecordSchema {
            name: Name { namespace, .. },
            aliases: Some(aliases),
            ..
        })
        | AvroSchema::Enum(EnumSchema {
            name: Name { namespace, .. },
            aliases: Some(aliases),
            ..
        })
        | AvroSchema::Fixed(FixedSchema {
            name: Name { namespace, .. },
            aliases: Some(aliases),
            ..
        }) => {
            let aliases: Vec<String> = aliases
                .iter()
                .map(|alias| aliased(alias, namespace.as_deref(), None))
                .collect();
            props.insert(
                "avro::aliases".to_string(),
                format!("[{}]", aliases.join(",")),
            );
        }
        _ => {}
    }
    props
}

/// Returns the fully qualified name for a field
pub fn aliased(alias: &Alias, namespace: Option<&str>, default_namespace: Option<&str>) -> String {
    if alias.namespace().is_some() {
        alias.fullname(None)
    } else {
        let namespace = namespace.as_ref().copied().or(default_namespace);

        match namespace {
            Some(ref namespace) => format!("{}.{}", namespace, alias.name()),
            None => alias.fullname(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::variant::VariantType;

    fn root_schema(field_type: &str) -> AvroSchema {
        AvroSchema::parse_str(&format!(
            r#"{{
                "type": "record",
                "name": "root",
                "fields": [
                    {{
                        "name": "var",
                        "type": {field_type}
                    }}
                ]
            }}"#
        ))
        .unwrap()
    }

    fn variant_record(fields: &str) -> String {
        format!(
            r#"{{
                "type": "record",
                "name": "variant_record",
                "logicalType": "variant",
                "fields": [{fields}]
            }}"#
        )
    }

    #[test]
    fn maps_hudi_variant_record_to_arrow_variant_extension() {
        let schema = root_schema(&variant_record(
            r#"
                {"name": "metadata", "type": "bytes"},
                {"name": "value", "type": "bytes"}
            "#,
        ));

        let arrow_schema = to_arrow_schema(&schema).unwrap();
        let field = arrow_schema.field_with_name("var").unwrap();

        field.try_extension_type::<VariantType>().unwrap();
        let DataType::Struct(fields) = field.data_type() else {
            panic!("variant should be represented as an Arrow struct");
        };
        assert_eq!(fields[0].name(), "metadata");
        assert_eq!(fields[1].name(), "value");
    }

    #[test]
    fn maps_nullable_hudi_variant_record_to_nullable_arrow_variant_extension() {
        let schema = root_schema(&format!(
            r#"[ "null", {} ]"#,
            variant_record(
                r#"
                    {"name": "metadata", "type": "bytes"},
                    {"name": "value", "type": "bytes"}
                "#,
            )
        ));

        let arrow_schema = to_arrow_schema(&schema).unwrap();
        let field = arrow_schema.field_with_name("var").unwrap();

        assert!(field.is_nullable());
        field.try_extension_type::<VariantType>().unwrap();
    }

    #[test]
    fn rejects_hudi_variant_record_without_metadata() {
        let schema = root_schema(&variant_record(
            r#"
                {"name": "value", "type": "bytes"}
            "#,
        ));

        let err = to_arrow_schema(&schema).unwrap_err();
        assert!(err.to_string().contains("must contain a 'metadata' field"));
    }

    #[test]
    fn rejects_hudi_variant_record_with_non_binary_value() {
        let schema = root_schema(&variant_record(
            r#"
                {"name": "metadata", "type": "bytes"},
                {"name": "value", "type": "string"}
            "#,
        ));

        let err = to_arrow_schema(&schema).unwrap_err();
        assert!(err.to_string().contains("must have binary 'value'"));
    }
}
