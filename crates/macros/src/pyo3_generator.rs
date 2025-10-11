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

//! PyO3 binding code generation for Rust enums.

use proc_macro2::TokenStream;
use quote::quote;

/// Generate PyO3 binding code for an enum wrapper struct.
///
/// This generates:
/// - A `value` getter property that returns the configuration key string via `AsRef<str>`
/// - An `all_variants` class method that returns all enum variants via `strum::IntoEnumIterator`  
/// - String representation methods (`__str__`, `__repr__`)
/// - Equality comparison method (`__eq__`)
/// - A utility method `get_class_attributes` for creating class attributes at runtime
pub fn generate_pyo3_binding(
    wrapper_name: &syn::Ident,
    inner_enum_type: &syn::Type,
) -> TokenStream {
    quote! {
        impl #wrapper_name {
            /// Get all enum variants as a map of SCREAMING_SNAKE_CASE names to instances.
            ///
            /// This is used for runtime class attribute injection in Python modules.
            pub fn get_class_attributes() -> std::collections::HashMap<&'static str, #wrapper_name> {
                use ::strum::IntoEnumIterator;
                let mut attrs = std::collections::HashMap::new();

                for variant in <#inner_enum_type>::iter() {
                    let variant_name = format!("{:?}", variant);
                    let const_name = Self::convert_to_screaming_snake_case(&variant_name);
                    // We need to leak the string to get a 'static reference
                    let static_name: &'static str = Box::leak(const_name.into_boxed_str());
                    attrs.insert(static_name, #wrapper_name { inner: variant });
                }

                attrs
            }

            /// Convert PascalCase variant names to SCREAMING_SNAKE_CASE.
            ///
            /// Example: `BaseFileFormat` -> `BASE_FILE_FORMAT`
            fn convert_to_screaming_snake_case(input: &str) -> String {
                let mut result = String::new();
                let mut chars = input.chars().peekable();

                while let Some(ch) = chars.next() {
                    if ch.is_uppercase() && !result.is_empty() {
                        // Add underscore before uppercase letters (except the first one)
                        if chars.peek().map_or(false, |next_ch| next_ch.is_lowercase()) {
                            result.push('_');
                        }
                    }
                    result.push(ch.to_uppercase().next().unwrap());
                }

                result
            }
        }

        #[::pyo3::pymethods]
        impl #wrapper_name {
            #[getter]
            fn value(&self) -> String {
                self.inner.as_ref().to_string()
            }

            /// Get all enum variants as a list.
            ///
            /// This is exposed as a Python class method that can be called as:
            /// `HudiTableConfig.all_variants()`
            #[classmethod]
            fn all_variants(_cls: &::pyo3::Bound<'_, ::pyo3::types::PyType>) -> Vec<#wrapper_name> {
                use ::strum::IntoEnumIterator;
                <#inner_enum_type>::iter()
                    .map(|variant| #wrapper_name { inner: variant })
                    .collect()
            }

            /// Python `repr()` representation.
            ///
            /// Returns a string like `PyHudiTableConfig(hoodie.table.name)`
            fn __repr__(&self) -> String {
                format!("{}({})", stringify!(#wrapper_name), self.value())
            }

            /// Python `str()` representation.
            ///
            /// Returns the configuration key string directly.
            fn __str__(&self) -> String {
                self.value()
            }

            /// Python equality comparison.
            ///
            /// Two enum instances are equal if they represent the same variant.
            fn __eq__(&self, other: &Self) -> bool {
                std::mem::discriminant(&self.inner) == std::mem::discriminant(&other.inner)
            }
        }
    }
}
