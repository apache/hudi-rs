/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Checks a write runs before it fences an instant: the table's version,
//! type and merge configs, the SET column rules, and which ordering the
//! table's merge mode gives the writer.

use super::*;

/// Writes support table versions 8 (1.0.x) and 9 (1.1.x/1.2) only. tv6 has
/// divergent log naming, rollback markers, and archival; refuse rather than
/// write a layout 0.x readers half-understand.
pub(crate) fn ensure_writable_table_version(table: &Table) -> Result<()> {
    let version: isize = table
        .hudi_configs
        .get_or_default(crate::config::table::HudiTableConfig::TableVersion)
        .into();
    if !(8..=9).contains(&version) {
        return Err(CoreError::Unsupported(format!(
            "writing to table version {version} is not supported; supported versions are 8 and 9"
        )));
    }
    Ok(())
}

/// SET may not target record-key or partition columns: changing a row's
/// identity would strand its index entry and diverge the partition value from
/// the path it physically lives in.
pub(super) fn ensure_set_columns_updatable(table: &Table, updates: &RecordBatch) -> Result<()> {
    let mut identity = configured_record_key_fields(&table.hudi_configs)?;
    let partition_fields: Vec<String> = table
        .hudi_configs
        .get_or_default(crate::config::table::HudiTableConfig::PartitionFields)
        .into();
    // A custom key generator records each field as `<name>:<type>`.
    identity.extend(
        partition_fields
            .into_iter()
            .map(|field| match field.split_once(':') {
                Some((name, _)) => name.to_string(),
                None => field,
            }),
    );
    for field in updates.schema().fields() {
        if identity.iter().any(|name| name == field.name()) {
            return Err(CoreError::Write(format!(
                "update cannot SET record-key or partition column '{}'",
                field.name()
            )));
        }
    }
    Ok(())
}

/// Pre-fence input validation shared by write verbs: batches must agree with
/// each other and (when the table already has data) with the table schema, and
/// must contain at least one row, all checked BEFORE an instant is fenced so
/// invalid input leaves no timeline trace. Returns the batches widened to the
/// table's nullability.
pub(super) async fn validate_write_input(
    table: &Table,
    batches: &[RecordBatch],
    verb: &str,
) -> Result<Vec<RecordBatch>> {
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if rows == 0 {
        return Err(CoreError::Write(format!(
            "{verb} requires at least one row"
        )));
    }
    let first = batches[0].schema();
    // Commit metadata carries the schema as Avro JSON; an inexpressible type
    // (e.g. UInt32) must fail here, before an instant is fenced and data is
    // written, not while building the commit.
    crate::write::append::arrow_schema_to_avro_json(
        &crate::write::append::strip_meta_fields_from_schema(&first),
    )?;
    for batch in &batches[1..] {
        if crate::write::align_batch_to_schema(batch, &first).is_none() {
            return Err(CoreError::Schema(format!(
                "all {verb} batches must share one schema"
            )));
        }
    }
    if table
        .timeline
        .get_latest_commit_timestamp_as_option()
        .is_some()
    {
        let table_schema = std::sync::Arc::new(table.get_schema().await?);
        for batch in batches {
            if crate::write::align_batch_to_schema(batch, &table_schema).is_none() {
                return Err(CoreError::Schema(format!(
                    "{verb} batch schema does not match the current table schema"
                )));
            }
        }
        return crate::write::widen_to_table_nullability(batches, &table_schema);
    }
    Ok(batches.to_vec())
}

pub(super) fn ensure_rewrite_supported(table: &Table) -> Result<()> {
    ensure_copy_on_write(table)?;
    ensure_supported_merge_configs(table)
}

pub(super) fn ensure_mor_merge_supported(table: &Table) -> Result<()> {
    ensure_supported_merge_configs(table)?;
    let populates_meta_fields: bool = table
        .hudi_configs
        .get_or_default(crate::config::table::HudiTableConfig::PopulatesMetaFields)
        .into();
    if !populates_meta_fields {
        return Err(CoreError::Unsupported(
            "MERGE_ON_READ upsert and delete require hoodie.populate.meta.fields=true".to_string(),
        ));
    }
    let strategy: String = table
        .hudi_configs
        .get_or_default(RecordMergeStrategy)
        .into();
    if RecordMergeStrategyValue::from_str(&strategy)?
        != RecordMergeStrategyValue::OverwriteWithLatest
    {
        return Err(CoreError::Unsupported(
            "MERGE_ON_READ upsert and delete require an upsertable merge mode".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn ensure_supported_merge_configs(table: &Table) -> Result<()> {
    // Payload classes are deprecated; writers only honor merge mode / strategy.
    // Reject CUSTOM merge and custom merger impls — support COMMIT_TIME, EVENT_TIME,
    // and append-only (derived strategy).
    let options = table.hudi_configs.as_options();
    if let Some(mode) = options.get("hoodie.record.merge.mode") {
        let normalized = mode.to_ascii_uppercase();
        match normalized.as_str() {
            "COMMIT_TIME_ORDERING" | "EVENT_TIME_ORDERING" => {}
            "CUSTOM" => {
                return Err(CoreError::Unsupported(
                    "writes do not support hoodie.record.merge.mode=CUSTOM".to_string(),
                ));
            }
            other => {
                return Err(CoreError::Unsupported(format!(
                    "writes only support COMMIT_TIME_ORDERING or EVENT_TIME_ORDERING merge modes, got '{other}'"
                )));
            }
        }
    }
    if let Some(merger) = options.get("hoodie.record.merger.impls")
        && !merger.trim().is_empty()
    {
        return Err(CoreError::Unsupported(
            "writes do not support custom hoodie.record.merger.impls".to_string(),
        ));
    }
    Ok(())
}

/// Whether writes merge by event time, resolved exactly as the reader resolves
/// it (`hoodie.record.merge.mode` when stated, else Java's inference from
/// payload class / merge strategy / ordering field). Deriving it from the
/// ordering field alone applied event-time semantics to COMMIT_TIME_ORDERING
/// tables — including every 0.x table upgraded with a precombine field — and
/// silently dropped updates Java would have kept.
pub(super) fn uses_event_time_merge(hudi_configs: &crate::config::HudiConfigs) -> Result<bool> {
    use crate::file_group::reader_v2::reader_context::MergeMode;
    // An unresolvable mode (exotic payload class) is left to the heuristic
    // below rather than failing a write that previously succeeded; writes with
    // a genuinely custom merger are refused by ensure_supported_merge_configs.
    if let Ok(mode) = crate::file_group::reader_v2::resolver::resolve_merge_mode(hudi_configs)
        && mode == MergeMode::CommitTimeOrdering
    {
        return Ok(false);
    }
    // Event-time merging needs an ordering field to compare; without one there
    // is nothing to order by and the newest write wins.
    let strategy: String = hudi_configs.get_or_default(RecordMergeStrategy).into();
    let strategy = RecordMergeStrategyValue::from_str(&strategy)?;
    Ok(strategy == RecordMergeStrategyValue::OverwriteWithLatest
        && hudi_configs
            .try_get(OrderingFields)?
            .map(|v| -> Vec<String> { v.into() })
            .is_some_and(|fields| fields.iter().any(|f| !f.trim().is_empty())))
}

/// The ordering fields a write compares, empty unless it merges by event time.
pub(super) fn event_time_ordering_fields(
    hudi_configs: &crate::config::HudiConfigs,
) -> Result<Vec<String>> {
    if !uses_event_time_merge(hudi_configs)? {
        return Ok(Vec::new());
    }
    Ok(hudi_configs
        .try_get(OrderingFields)?
        .map(Into::into)
        .unwrap_or_default())
}

/// A partial update under event-time ordering must set every ordering field:
/// the merged row carries the winner's ordering value, or a later write
/// ordered between the two would beat the newer data.
pub(super) fn ensure_partial_update_sets_ordering(
    hudi_configs: &crate::config::HudiConfigs,
    update_columns: &[String],
) -> Result<()> {
    if let Some(field) = event_time_ordering_fields(hudi_configs)?
        .into_iter()
        .find(|field| !update_columns.contains(field))
    {
        return Err(CoreError::Write(format!(
            "a partial update under event-time ordering must include the ordering field '{field}' in its update columns"
        )));
    }
    Ok(())
}
