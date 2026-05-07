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
 */

//! Mirrors Java `org.apache.hudi.common.table.log.KeySpec` hierarchy
//! (`KeySpec`, `FullKeySpec`, `PrefixKeySpec`) plus
//! `HoodieMergedLogRecordReader.createKeySpec(Option<Predicate>)`.

use crate::expression::predicate::{Predicate, PredicateKind};
use crate::expression::{Expression, ExpressionKind, LiteralValue};
use std::sync::Arc;

/// Mirrors Java's `KeySpec` interface, with `FullKeySpec` and `PrefixKeySpec`
/// flattened into enum variants.
#[derive(Debug, Clone)]
pub enum KeySpec {
    /// Full keys list (from `Predicates::In`). Mirrors Java `FullKeySpec`.
    FullKeys(Vec<String>),
    /// Prefix list (from `Predicates::StringStartsWithAny`). Mirrors Java `PrefixKeySpec`.
    PrefixKeys(Vec<String>),
}

impl KeySpec {
    /// Test the given record-key string against this key-spec.
    pub fn matches(&self, key: &str) -> bool {
        match self {
            KeySpec::FullKeys(keys) => keys.iter().any(|k| k == key),
            KeySpec::PrefixKeys(prefixes) => prefixes.iter().any(|p| key.starts_with(p.as_str())),
        }
    }
}

/// Mirrors Java `HoodieMergedLogRecordReader.createKeySpec(Option<Predicate>)`,
/// implemented in idiomatic Rust via `PredicateKind` pattern matching instead
/// of Java's `instanceof`/cast.
pub fn create_key_spec(filter: Option<&dyn Predicate>) -> Option<KeySpec> {
    match filter?.pred_kind() {
        PredicateKind::In(p) => Some(KeySpec::FullKeys(string_literals(p.right_children())?)),
        PredicateKind::StringStartsWithAny(p) => {
            Some(KeySpec::PrefixKeys(string_literals(p.right_children())?))
        }
        _ => None,
    }
}

/// Extract `String` values from a list of `Literal(LiteralValue::String(_))`
/// expressions. Returns `None` if any child is not a String literal.
pub(crate) fn string_literals(exprs: &[Box<dyn Expression>]) -> Option<Vec<String>> {
    exprs
        .iter()
        .map(|e| match e.kind() {
            ExpressionKind::Literal(lit) => match lit.value() {
                LiteralValue::String(s) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// Convenience: accept an `Option<Arc<dyn Predicate>>` directly (the type
/// stored on `ReaderContext`).
pub fn create_key_spec_from_arc(filter: &Option<Arc<dyn Predicate>>) -> Option<KeySpec> {
    create_key_spec(filter.as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::predicates::predicates_factory;
    use crate::expression::{Literal, NameReference};

    #[test]
    fn create_key_spec_none_when_filter_is_none() {
        assert!(create_key_spec(None).is_none());
    }

    #[test]
    fn create_key_spec_in_predicate_yields_full_keys() {
        let p = predicates_factory::in_(
            Box::new(NameReference::new("_hoodie_record_key")),
            vec![
                Box::new(Literal::string("k1")) as Box<dyn Expression>,
                Box::new(Literal::string("k2")) as Box<dyn Expression>,
            ],
        );
        let spec = create_key_spec(Some(&p)).expect("expected Some(KeySpec)");
        match spec {
            KeySpec::FullKeys(keys) => assert_eq!(keys, vec!["k1".to_string(), "k2".to_string()]),
            _ => panic!("expected FullKeys"),
        }
    }

    #[test]
    fn create_key_spec_starts_with_any_yields_prefix_keys() {
        let p = predicates_factory::starts_with_any(
            Box::new(NameReference::new("_hoodie_record_key")),
            vec![
                Box::new(Literal::string("alpha")) as Box<dyn Expression>,
                Box::new(Literal::string("beta")) as Box<dyn Expression>,
            ],
        );
        let spec = create_key_spec(Some(&p)).expect("expected Some(KeySpec)");
        match spec {
            KeySpec::PrefixKeys(prefixes) => {
                assert_eq!(prefixes, vec!["alpha".to_string(), "beta".to_string()])
            }
            _ => panic!("expected PrefixKeys"),
        }
    }

    #[test]
    fn create_key_spec_returns_none_for_non_in_non_starts_with() {
        let p = predicates_factory::eq(
            Box::new(Literal::string("a")),
            Box::new(Literal::string("b")),
        );
        assert!(create_key_spec(Some(&p)).is_none());
    }

    #[test]
    fn full_keys_matches() {
        let spec = KeySpec::FullKeys(vec!["a".into(), "b".into()]);
        assert!(spec.matches("a"));
        assert!(spec.matches("b"));
        assert!(!spec.matches("c"));
    }

    #[test]
    fn prefix_keys_matches() {
        let spec = KeySpec::PrefixKeys(vec!["pre".into(), "alpha".into()]);
        assert!(spec.matches("prefix-1"));
        assert!(spec.matches("alphabet"));
        assert!(!spec.matches("beta"));
    }
}
