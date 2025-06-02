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
#include <iostream>
#include <string>
#include <cstring>
#include <iomanip>
#include <cstdlib>
#include "cxx.h"
#include "src/lib.rs.h"
#include "arrow/c/abi.h"


// Function to print schema format info
void PrintArrowSchema(const ArrowSchema* schema, int indent = 0) {
    // Print indentation
    std::string indentation(indent * 2, ' ');

    // Print schema details
    std::cout << indentation << "Format: " << (schema->format ? schema->format : "NULL") << std::endl;
    std::cout << indentation << "Name: " << (schema->name ? schema->name : "NULL") << std::endl;

    // Print flags
    std::cout << indentation << "Flags: ";
    if (schema->flags & ARROW_FLAG_NULLABLE) std::cout << "NULLABLE ";
    if (schema->flags & ARROW_FLAG_DICTIONARY_ORDERED) std::cout << "DICTIONARY_ORDERED ";
    if (schema->flags & ARROW_FLAG_MAP_KEYS_SORTED) std::cout << "MAP_KEYS_SORTED ";
    std::cout << "(" << schema->flags << ")" << std::endl;

    // Print metadata if available
    if (schema->metadata && strlen(schema->metadata) > 0) {
        std::cout << indentation << "Metadata: " << schema->metadata << std::endl;
    } else {
        std::cout << indentation << "Metadata: NULL" << std::endl;
    }

    // Print children
    std::cout << indentation << "Children: " << schema->n_children << std::endl;
    for (int64_t i = 0; i < schema->n_children; i++) {
        std::cout << indentation << "Child " << i << ":" << std::endl;
        PrintArrowSchema(schema->children[i], indent + 1);
    }

    // Print dictionary if available
    if (schema->dictionary) {
        std::cout << indentation << "Dictionary:" << std::endl;
        PrintArrowSchema(schema->dictionary, indent + 1);
    }
}

// Maps Arrow format strings to human-readable type names
std::string GetTypeNameFromFormat(const char* format) {
    if (!format) return "NULL";

    // Common format strings
    if (strcmp(format, "n") == 0) return "null";
    if (strcmp(format, "b") == 0) return "bool";
    if (strcmp(format, "c") == 0) return "int8";
    if (strcmp(format, "C") == 0) return "uint8";
    if (strcmp(format, "s") == 0) return "int16";
    if (strcmp(format, "S") == 0) return "uint16";
    if (strcmp(format, "i") == 0) return "int32";
    if (strcmp(format, "I") == 0) return "uint32";
    if (strcmp(format, "l") == 0) return "int64";
    if (strcmp(format, "L") == 0) return "uint64";
    if (strcmp(format, "e") == 0) return "float16";
    if (strcmp(format, "f") == 0) return "float32";
    if (strcmp(format, "g") == 0) return "float64";
    if (strcmp(format, "u") == 0) return "utf8_string";
    if (strcmp(format, "U") == 0) return "large_utf8_string";
    if (strcmp(format, "z") == 0) return "binary";
    if (strcmp(format, "Z") == 0) return "large_binary";
    if (strcmp(format, "d") == 0) return "decimal128";
    if (strcmp(format, "w") == 0) return "fixed_size_binary";
    if (strcmp(format, "tdD") == 0) return "date32";
    if (strcmp(format, "tdm") == 0) return "date64";
    if (strcmp(format, "tts") == 0) return "time32[second]";
    if (strcmp(format, "ttm") == 0) return "time32[millisecond]";
    if (strcmp(format, "ttu") == 0) return "time64[microsecond]";
    if (strcmp(format, "ttn") == 0) return "time64[nanosecond]";
    if (strcmp(format, "tss") == 0) return "timestamp[second]";
    if (strcmp(format, "tsm") == 0) return "timestamp[millisecond]";
    if (strcmp(format, "tsu") == 0) return "timestamp[microsecond]";
    if (strcmp(format, "tsn") == 0) return "timestamp[nanosecond]";
    if (strcmp(format, "+l") == 0) return "list";
    if (strcmp(format, "+L") == 0) return "large_list";
    if (strcmp(format, "+s") == 0) return "struct";
    if (strcmp(format, "+m") == 0) return "map";

    // If not a known format, return the format string itself
    return std::string(format);
}

// Function to print schema in a more readable table-like format
void PrintSchemaTable(const ArrowSchema* schema) {
    std::cout << "\n===== Schema Table =====\n" << std::endl;
    std::cout << "Index | Name                 | Type                 | Nullable" << std::endl;
    std::cout << "------+----------------------+----------------------+----------" << std::endl;

    // Print each field
    for (int64_t i = 0; i < schema->n_children; i++) {
        const ArrowSchema* field = schema->children[i];
        std::string name = field->name ? field->name : "NULL";
        if (name.length() > 20) name = name.substr(0, 17) + "...";

        std::string type = GetTypeNameFromFormat(field->format);
        if (type.length() > 20) type = type.substr(0, 17) + "...";

        bool nullable = (field->flags & ARROW_FLAG_NULLABLE) != 0;

        std::cout << std::setw(5) << i << " | "
                  << std::setw(20) << std::left << name << " | "
                  << std::setw(20) << std::left << type << " | "
                  << std::setw(8) << std::left << (nullable ? "Yes" : "No")
                  << std::endl;
    }
    std::cout << std::endl;
}

int main() {
    try {
        std::cout << "Getting Hudi table schema and record batch information..." << std::endl;

        auto base_uri = "s3://hudi-demo/cow/v6_nonpartitioned";

        std::vector<std::string> opts{};
        auto file_group_reader = new_file_group_reader_with_options(base_uri, opts);

        auto base_file_path = "a079bdb3-731c-4894-b855-abfcd6921007-0_0-203-274_20240418173551906.parquet";
        ArrowArrayStream* stream_ptr = file_group_reader->read_file_slice_by_base_file_path(base_file_path);

        if (!stream_ptr) {
            std::cerr << "Error: Received null stream pointer" << std::endl;
            return 1;
        }

        ArrowSchema c_schema;
        if (stream_ptr->get_schema(stream_ptr, &c_schema) != 0) {
            std::string error = "Error getting schema: ";
            const char* last_error = stream_ptr->get_last_error(stream_ptr);
            if (last_error) error += last_error;
            throw std::runtime_error(error);
        }

        std::cout << "\n===== Schema Details =====\n" << std::endl;
        PrintArrowSchema(&c_schema);

        PrintSchemaTable(&c_schema);

        std::cout << "\n===== Record Batch Information =====\n" << std::endl;
        int batch_count = 0;
        int64_t total_rows = 0;

        while (true) {
            ArrowArray c_array;
            if (stream_ptr->get_next(stream_ptr, &c_array) != 0) {
                std::string error = "Error getting next batch: ";
                const char* last_error = stream_ptr->get_last_error(stream_ptr);
                if (last_error) error += last_error;
                throw std::runtime_error(error);
            }

            // Check if we've reached the end of the stream
            if (c_array.release == nullptr) {
                std::cout << "End of stream reached." << std::endl;
                break;
            }

            batch_count++;
            std::cout << "Record Batch #" << batch_count << ":" << std::endl;
            std::cout << "  Rows: " << c_array.length << std::endl;
            std::cout << "  Null count: " << c_array.null_count << std::endl;
            std::cout << "  Offset: " << c_array.offset << std::endl;
            std::cout << "  Number of buffers: " << c_array.n_buffers << std::endl;
            std::cout << "  Number of children: " << c_array.n_children << std::endl;

            total_rows += c_array.length;

            c_array.release(&c_array);
        }

        std::cout << "\nSummary:" << std::endl;
        std::cout << "  Total record batches: " << batch_count << std::endl;
        std::cout << "  Total rows: " << total_rows << std::endl;

        bool validation_passed = true;
        if (batch_count != 1) {
            std::cerr << "Validation failed: Expected exactly 1 record batch, got " << batch_count << std::endl;
            validation_passed = false;
        }
        if (total_rows != 4) {
            std::cerr << "Validation failed: Expected exactly 4 rows, got " << total_rows << std::endl;
            validation_passed = false;
        }

        if (c_schema.release) {
            c_schema.release(&c_schema);
        }

        stream_ptr->release(stream_ptr);

        if (!validation_passed) {
            std::cerr << "Data validation failed - resources cleaned up properly" << std::endl;
            return 1;
        }

        std::cout << "\nData validation successful - all resources cleaned up" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
