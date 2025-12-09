//! Observability configuration for the durable execution system.
//!
//! This module provides opt-in telemetry including:
//! - OpenTelemetry distributed tracing (export to Jaeger, Tempo, etc.)
//! - Prometheus metrics export
//! - W3C Trace Context propagation across process boundaries
//!
//! # Feature Flag
//!
//! Enable with the `telemetry` feature:
//! ```toml
//! durable = { version = "0.1", features = ["telemetry"] }
//! ```
//!
//! # Example
//!
//! ```ignore
//! use durable::telemetry::TelemetryBuilder;
//!
//! let telemetry = TelemetryBuilder::new()
//!     .service_name("my-service")
//!     .otlp_endpoint("http://localhost:4317")
//!     .prometheus_addr("0.0.0.0:9090".parse()?)
//!     .build()?;
//!
//! // ... run your application ...
//!
//! telemetry.shutdown().await;
//! ```

mod config;
mod metrics;
mod propagation;

pub use config::{TelemetryBuilder, TelemetryHandle};
pub use metrics::*;
pub use propagation::{extract_trace_context, inject_trace_context};
