//! Observability helpers for the durable execution system.
//!
//! This module provides:
//! - Metric recording helpers (backend-agnostic via the `metrics` crate)
//! - W3C Trace Context propagation across task boundaries
//!
//! # Feature Flag
//!
//! Enable with the `telemetry` feature:
//! ```toml
//! durable = { version = "0.1", features = ["telemetry"] }
//! ```
//!
//! # Usage
//!
//! This module does **not** set up exporters. You must configure your own
//! tracing subscriber and metrics recorder in your application. The library
//! will emit metrics and propagate trace context automatically.

mod metrics;
mod propagation;

pub use metrics::*;
pub use propagation::{extract_trace_context, inject_trace_context};
