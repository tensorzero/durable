//! W3C Trace Context propagation for distributed tracing across process boundaries.
//!
//! This module enables trace context to flow from task spawners to task executors,
//! even when they run on different machines communicating via PostgreSQL.
//!
//! The trace context is serialized using the W3C Trace Context standard format:
//! `traceparent: 00-{trace_id}-{span_id}-{flags}`

use opentelemetry::Context;
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const TRACEPARENT: &str = "traceparent";

#[allow(dead_code)]
const TRACESTATE: &str = "tracestate";

/// Wrapper to implement `Injector` for HashMap<String, JsonValue>
struct HashMapInjector<'a>(&'a mut HashMap<String, JsonValue>);

impl Injector for HashMapInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), JsonValue::String(value));
    }
}

/// Wrapper to implement `Extractor` for HashMap<String, JsonValue>
struct HashMapExtractor<'a>(&'a HashMap<String, JsonValue>);

impl Extractor for HashMapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// Inject the current span's trace context into a headers map.
///
/// This should be called at task spawn time to capture the caller's trace context.
/// The trace context is stored as `traceparent` and optionally `tracestate` keys.
///
/// # Example
///
/// ```ignore
/// let mut headers = HashMap::new();
/// inject_trace_context(&mut headers);
/// // headers now contains {"traceparent": "00-...-...-01"}
/// ```
pub fn inject_trace_context(headers: &mut HashMap<String, JsonValue>) {
    let propagator = TraceContextPropagator::new();
    let cx = tracing::Span::current().context();
    let mut injector = HashMapInjector(headers);
    propagator.inject_context(&cx, &mut injector);
}

/// Extract trace context from a headers map.
///
/// This should be called at task execution time to restore the caller's trace context.
/// The returned `Context` can be used to set the parent of a new span.
///
/// # Example
///
/// ```ignore
/// let cx = extract_trace_context(&task.headers);
/// let span = info_span!("task.execute");
/// span.set_parent(cx);
/// ```
pub fn extract_trace_context(headers: &HashMap<String, JsonValue>) -> Context {
    let propagator = TraceContextPropagator::new();
    let extractor = HashMapExtractor(headers);
    propagator.extract(&extractor)
}

/// Check if headers contain trace context.
#[allow(dead_code)]
pub fn has_trace_context(headers: &HashMap<String, JsonValue>) -> bool {
    headers.contains_key(TRACEPARENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inject_extract_roundtrip() {
        // Note: This test verifies the basic mechanics work.
        // Full integration testing requires an active OpenTelemetry context.
        let mut headers = HashMap::new();

        // Without an active span, inject should still work (just won't have context)
        inject_trace_context(&mut headers);

        // Extract should return a valid (possibly empty) context
        let _cx = extract_trace_context(&headers);
    }

    #[test]
    fn test_has_trace_context() {
        let mut headers = HashMap::new();
        assert!(!has_trace_context(&headers));

        headers.insert(
            TRACEPARENT.to_string(),
            JsonValue::String("00-abc-def-01".to_string()),
        );
        assert!(has_trace_context(&headers));
    }

    #[test]
    fn test_extractor_keys() {
        let mut headers = HashMap::new();
        headers.insert("key1".to_string(), JsonValue::String("value1".to_string()));
        headers.insert("key2".to_string(), JsonValue::String("value2".to_string()));

        let extractor = HashMapExtractor(&headers);
        let keys = extractor.keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"key1"));
        assert!(keys.contains(&"key2"));
    }
}
