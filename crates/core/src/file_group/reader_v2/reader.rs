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

//! The file group reader.
//!
//! Only the entry point exists so far. It is here ahead of the engine so that
//! the shape callers will use is settled, and so the gap is something tests can
//! point at rather than an absence.
//!
//! Shares a name with [`crate::file_group::reader::FileGroupReader`], which
//! reads file groups today. This one replaces it once the engine behind it is
//! written, so it carries the name it will keep; until then, reach for either
//! by module path. Nothing outside this module uses this one yet.

use crate::Result;
use crate::error::CoreError;
use crate::file_group::reader_v2::reader_context::ReaderContext;
use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
use arrow_array::RecordBatch;

/// Reads one file slice.
///
/// Serves both table types: a merge-on-read slice merges its log records onto
/// the base file, and a copy-on-write slice is the same read with no log files
/// to merge.
#[allow(dead_code)]
pub(crate) struct FileGroupReader {
    context: ReaderContext,
    parameters: ReaderParameters,
}

#[allow(dead_code)]
impl FileGroupReader {
    pub(crate) fn new(context: ReaderContext, parameters: ReaderParameters) -> Self {
        Self {
            context,
            parameters,
        }
    }

    /// What the reader was resolved to read.
    pub(crate) fn context(&self) -> &ReaderContext {
        &self.context
    }

    /// Flags controlling what this reader emits.
    pub(crate) fn parameters(&self) -> &ReaderParameters {
        &self.parameters
    }

    /// Read the file slice and return the merged rows.
    ///
    /// # Errors
    /// Always, for now: the merge engine has not been written. Failing is the
    /// point — an empty batch would be indistinguishable from a file slice that
    /// genuinely has no rows, and would let an unfinished reader look like it
    /// worked.
    pub(crate) async fn read(&self) -> Result<RecordBatch> {
        Err(CoreError::Unsupported(
            "The merge-on-read file group reader is not yet implemented. \
             Its context resolves, but no merge engine is wired up behind it."
                .to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HudiConfigs;
    use crate::config::read::HudiReadConfig;
    use crate::config::table::HudiTableConfig;
    use crate::file_group::reader_v2::reader_parameters::ReaderParameters;
    use crate::file_group::reader_v2::resolver::resolve_reader_context;

    fn context() -> ReaderContext {
        let configs = HudiConfigs::new([
            (HudiTableConfig::BasePath.as_ref(), "file:///tmp/t"),
            (HudiReadConfig::EndTimestamp.as_ref(), "20240101000000000"),
            (HudiTableConfig::OrderingFields.as_ref(), "ts"),
        ]);
        resolve_reader_context(&configs, true).unwrap()
    }

    /// The context resolves, but there is no engine behind it yet. Reading must
    /// say so plainly — returning an empty batch would look like a table with no
    /// rows, which is indistinguishable from a real answer.
    #[tokio::test]
    async fn read_reports_that_the_engine_is_not_implemented() {
        let reader = FileGroupReader::new(context(), ReaderParameters::default());

        let err = reader.read().await.unwrap_err();

        assert!(
            err.to_string().contains("not yet implemented"),
            "error should say the engine is missing, got: {err}"
        );
    }

    /// The reader hands back what it was built with, rather than re-deriving or
    /// defaulting it. Uses non-default parameters so that substituting defaults
    /// somewhere in construction would fail this rather than pass unnoticed.
    #[test]
    fn holds_the_context_and_parameters_it_was_built_with() {
        let parameters = ReaderParameters {
            emit_delete: true,
            sort_output: true,
            allow_inflight_instants: true,
            ..Default::default()
        };

        let reader = FileGroupReader::new(context(), parameters.clone());

        assert_eq!(reader.context().table_path, "file:///tmp/t");
        assert_eq!(
            format!("{:?}", reader.parameters()),
            format!("{parameters:?}")
        );
    }
}
