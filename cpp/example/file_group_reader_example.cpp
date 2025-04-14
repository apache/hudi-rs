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

#include "hudi/file_group_reader.h"
#include <arrow/c/bridge.h>  // For Arrow C++ utilities to work with C Data Interface
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <iostream>
#include <memory>
#include <vector>

// Utility function to process data from an ArrowArrayStream
void processArrowStream(struct ArrowArrayStream* stream) {
    // Step 1: Get schema
    struct ArrowSchema schema;
    int err = stream->get_schema(stream, &schema);
    if (err != 0) {
        std::cerr << "Error getting schema: " << stream->get_last_error(stream) << std::endl;
        return;
    }

    // Import schema using Arrow C++ Bridge
    auto result_schema = arrow::ImportSchema(&schema);
    if (!result_schema.ok()) {
        std::cerr << "Error importing schema: " << result_schema.status().ToString() << std::endl;
        schema.release(&schema);
        return;
    }
    auto arrow_schema = result_schema.ValueOrDie();

    // Print schema
    std::cout << "Schema:\n" << arrow_schema->ToString() << std::endl;

    // Step 2: Read record batches
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    while (true) {
        struct ArrowArray array;
        err = stream->get_next(stream, &array);
        if (err != 0) {
            std::cerr << "Error getting next array: " << stream->get_last_error(stream) << std::endl;
            break;
        }

        // Check if end of stream
        if (array.release == nullptr) {
            break;
        }

        // Import record batch
        auto result_batch = arrow::ImportRecordBatch(&array, arrow_schema);
        if (!result_batch.ok()) {
            std::cerr << "Error importing record batch: " << result_batch.status().ToString() << std::endl;
            array.release(&array);
            continue;
        }

        batches.push_back(result_batch.ValueOrDie());
    }

    // Step 3: Convert batches to table and print summary
    auto result_table = arrow::Table::FromRecordBatches(batches);
    if (!result_table.ok()) {
        std::cerr << "Error creating table: " << result_table.status().ToString() << std::endl;
        return;
    }

    auto table = result_table.ValueOrDie();
    std::cout << "Successfully read " << table->num_rows() << " rows and "
              << table->num_columns() << " columns." << std::endl;

    // Print first few column names
    std::cout << "Columns: ";
    const int max_cols_to_show = 5;
    for (int i = 0; i < std::min(max_cols_to_show, table->num_columns()); i++) {
        std::cout << table->column_names()[i];
        if (i < std::min(max_cols_to_show, table->num_columns()) - 1) {
            std::cout << ", ";
        }
    }
    if (table->num_columns() > max_cols_to_show) {
        std::cout << ", ...";
    }
    std::cout << std::endl;
}

int main() {
    // Hard-coded path to a Hudi table
    std::string table_path = "/path/to/hudi/table";

    // Hard-coded relative file path within the table
    std::string file_path = "a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet";

    try {
        // Create optional parameters for the reader
        std::vector<std::string> options = {
            "hoodie.internal.schema.share.enable=true",
            "hoodie.read.use.read.optimized.mode=true"
        };

        // Create the file group reader
        hudi::FileGroupReader reader(table_path, options);

        // Read a file slice
        std::cout << "Reading file slice: " << file_path << std::endl;
        struct ArrowArrayStream* stream = reader.readFileSliceByPath(file_path);

        // Process the Arrow stream
        processArrowStream(stream);

        // Release the stream when done
        stream->release(stream);

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}