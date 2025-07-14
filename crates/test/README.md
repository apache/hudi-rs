<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# The `test` crate

This is a crate meant for facilitating testing in `hudi-rs` - it provides utilities and test data.

The `data/` directory contains fully prepared sample Hudi tables categorized by table version and configuration options:
each zipped file contains the Hudi table and the corresponding `.sql` contains the SQL used to generate the table.

Enums in `src/lib.rs` like `QuickstartTripsTable` and `SampleTable` are for conveniently accessing these ready-made
tables in tests. They take care of the unzipping and hand over the table paths to callers.
