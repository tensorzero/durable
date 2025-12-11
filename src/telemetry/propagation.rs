//! W3C Trace Context propagation for distributed tracing across process boundaries.
//!
//! This module enables trace context to flow from task spawners to task executors,
//! even when they run on different machines communicating via PostgreSQL.
//!
//! The trace context is serialized using the W3C Trace Context standard format:
//! `traceparent: 00-{trace_id}-{span_id}-{flags}`
//!
//! The trace context is stored under a single namespaced key `durable::otel_context`
//! as a serialized JSON object containing the W3C headers.

use opentelemetry::Context;
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Key used to store OTEL context in the headers map
const OTEL_CONTEXT_KEY: &str = "durable::otel_context";

/// Wrapper to implement `Injector` for HashMap<String, String>
struct HashMapInjector<'a>(&'a mut HashMap<String, String>);

impl Injector for HashMapInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

/// Wrapper to implement `Extractor` for HashMap<String, String>
struct HashMapExtractor<'a>(&'a HashMap<String, String>);

impl Extractor for HashMapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|s| s.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// Inject the current span's trace context into a headers map.
///
/// This should be called at task spawn time to capture the caller's trace context.
/// The trace context is stored as a JSON object under the `durable::otel_context` key.
///
/// # Example
///
/// ```ignore
/// let mut headers = HashMap::new();
/// inject_trace_context(&mut headers);
/// // headers now contains {"durable::otel_context": {"traceparent": "00-...-...-01"}}
/// ```
pub fn inject_trace_context(headers: &mut HashMap<String, JsonValue>) {
    let propagator = TraceContextPropagator::new();
    let cx = tracing::Span::current().context();

    // Inject into a temporary HashMap<String, String>
    let mut otel_headers = HashMap::new();
    let mut injector = HashMapInjector(&mut otel_headers);
    propagator.inject_context(&cx, &mut injector);

    // Only store if there's actual context to propagate
    if !otel_headers.is_empty()
        && let Ok(json_value) = serde_json::to_value(otel_headers)
    {
        headers.insert(OTEL_CONTEXT_KEY.to_string(), json_value);
    }
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

    // Extract the OTEL context from the namespaced key
    let otel_headers: HashMap<String, String> = headers
        .get(OTEL_CONTEXT_KEY)
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    let extractor = HashMapExtractor(&otel_headers);
    propagator.extract(&extractor)
}

/// Check if headers contain trace context.
#[allow(dead_code)]
pub fn has_trace_context(headers: &HashMap<String, JsonValue>) -> bool {
    headers.contains_key(OTEL_CONTEXT_KEY)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
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

        // Insert a properly structured OTEL context
        let mut otel_context = HashMap::new();
        otel_context.insert("traceparent".to_string(), "00-abc-def-01".to_string());
        let json_value = serde_json::to_value(otel_context).unwrap();
        headers.insert(OTEL_CONTEXT_KEY.to_string(), json_value);
        assert!(has_trace_context(&headers));
    }

    #[test]
    fn test_extractor_keys() {
        let mut headers = HashMap::new();
        headers.insert("key1".to_string(), "value1".to_string());
        headers.insert("key2".to_string(), "value2".to_string());

        let extractor = HashMapExtractor(&headers);
        let keys = extractor.keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"key1"));
        assert!(keys.contains(&"key2"));
    }
}
