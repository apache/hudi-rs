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

//! Procedural macros for generating language bindings from Hudi Rust enums.
//!
//! This crate provides a unified approach to automatically generate language binding code
//! from canonical Rust enum definitions, eliminating the need for manual duplication
//! across different language bindings.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

mod pyo3_generator;

/// Automatically generate language bindings for Rust enums.
///
/// This macro supports multiple language binding targets via the `#[auto_bind(...)]` attribute.
/// Currently supported targets:
/// - `pyo3`: Generate PyO3 Python bindings
///
/// # Example
///
/// ```rust,ignore
/// use hudi_bindings_macros::AutoBind;
/// use pyo3::prelude::*;
///
/// #[derive(Clone, Debug, AutoBind)]
/// #[auto_bind(pyo3)]
/// #[pyclass(name = "HudiTableConfig")]
/// pub struct PyHudiTableConfig {
///     inner: HudiTableConfig,
/// }
/// ```
///
/// # Requirements
///
/// The inner enum must:
/// - Implement `strum::IntoEnumIterator` (via `#[derive(EnumIter)]`)
/// - Implement `AsRef<str>` for string conversion
/// - Implement `Debug` for variant name extraction
///
/// The wrapper struct must:
/// - Have an `inner` field containing the enum
/// - Include appropriate binding-specific attributes (e.g., `#[pyclass]` for PyO3)
#[proc_macro_derive(AutoBind, attributes(auto_bind))]
pub fn derive_auto_bind(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let binding_targets = extract_binding_targets(&input);

    if binding_targets.is_empty() {
        panic!("AutoBind macro requires at least one #[auto_bind(...)] attribute. Supported targets: pyo3");
    }

    let wrapper_name = &input.ident;

    let inner_enum_type = extract_inner_enum_type(&input);

    let mut generated_code = quote! {};

    for target in binding_targets {
        match target.as_str() {
            "pyo3" => {
                let pyo3_code =
                    pyo3_generator::generate_pyo3_binding(wrapper_name, inner_enum_type);
                generated_code.extend(pyo3_code);
            }
            _ => panic!(
                "Unsupported binding target: {}. Supported targets: pyo3",
                target
            ),
        }
    }

    TokenStream::from(generated_code)
}

/// Extract binding targets from #[auto_bind(...)] attributes
fn extract_binding_targets(input: &DeriveInput) -> Vec<String> {
    let mut targets = Vec::new();

    for attr in &input.attrs {
        if !attr.path().is_ident("auto_bind") {
            continue;
        }

        match &attr.meta {
            syn::Meta::Path(_) => {
                targets.push("pyo3".to_string());
            }
            syn::Meta::List(meta_list) => {
                // Handle #[auto_bind(pyo3, jni, etc.)]
                let result = meta_list.parse_args_with(
                    syn::punctuated::Punctuated::<syn::Path, syn::Token![,]>::parse_terminated,
                );
                if let Ok(paths) = result {
                    for path in paths {
                        if let Some(ident) = path.get_ident() {
                            targets.push(ident.to_string());
                        }
                    }
                }
            }
            syn::Meta::NameValue(_) => {
                panic!("auto_bind attribute does not support name-value syntax");
            }
        }
    }

    targets
}

/// Extract the inner enum type from the wrapper struct
fn extract_inner_enum_type(input: &DeriveInput) -> &syn::Type {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => {
                fields
                    .named
                    .iter()
                    .find(|field| {
                        field
                            .ident
                            .as_ref()
                            .map(|ident| ident == "inner")
                            .unwrap_or(false)
                    })
                    .map(|field| &field.ty)
                    .expect("AutoBind requires a struct with an 'inner' field")
            }
            _ => panic!("AutoBind requires a struct with named fields"),
        },
        _ => panic!("AutoBind can only be used on structs"),
    }
}
