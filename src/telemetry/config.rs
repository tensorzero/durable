//! Telemetry configuration and initialization.

use crate::telemetry::metrics::register_metrics;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{KeyValue, trace::TraceError};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource, runtime,
    trace::{RandomIdGenerator, Sampler, TracerProvider},
};
use std::net::SocketAddr;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Error type for telemetry initialization failures.
#[derive(Debug, thiserror::Error)]
#[allow(clippy::enum_variant_names)]
pub enum TelemetryError {
    #[error("Failed to initialize OpenTelemetry tracer: {0}")]
    TracerInit(#[from] TraceError),
    #[error("Failed to initialize Prometheus exporter: {0}")]
    PrometheusInit(String),
    #[error("Failed to set global subscriber: {0}")]
    SubscriberInit(#[from] tracing_subscriber::util::TryInitError),
}

/// Builder for configuring telemetry.
///
/// # Example
///
/// ```ignore
/// let telemetry = TelemetryBuilder::new()
///     .service_name("my-service")
///     .otlp_endpoint("http://localhost:4317")
///     .prometheus_addr("0.0.0.0:9090".parse()?)
///     .build()?;
/// ```
pub struct TelemetryBuilder {
    service_name: String,
    otlp_endpoint: Option<String>,
    prometheus_addr: Option<SocketAddr>,
}

impl Default for TelemetryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TelemetryBuilder {
    /// Create a new telemetry builder with default settings.
    pub fn new() -> Self {
        Self {
            service_name: "durable".to_string(),
            otlp_endpoint: None,
            prometheus_addr: None,
        }
    }

    /// Set the service name for OpenTelemetry traces.
    pub fn service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    /// Set the OTLP endpoint for exporting traces.
    ///
    /// Example: `"http://localhost:4317"` for a local Jaeger or OTEL collector.
    pub fn otlp_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.otlp_endpoint = Some(endpoint.into());
        self
    }

    /// Set the address for the Prometheus metrics endpoint.
    ///
    /// Example: `"0.0.0.0:9090".parse()?` to expose metrics on port 9090.
    pub fn prometheus_addr(mut self, addr: SocketAddr) -> Self {
        self.prometheus_addr = Some(addr);
        self
    }

    /// Build and initialize the telemetry subsystems.
    ///
    /// This will:
    /// 1. Set up OpenTelemetry tracing (if `otlp_endpoint` is configured)
    /// 2. Set up Prometheus metrics (if `prometheus_addr` is configured)
    /// 3. Install the tracing subscriber
    ///
    /// Returns a `TelemetryHandle` that should be used for graceful shutdown.
    pub fn build(self) -> Result<TelemetryHandle, TelemetryError> {
        // Set up OpenTelemetry tracing if endpoint is configured
        let tracer_provider = if let Some(endpoint) = &self.otlp_endpoint {
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()?;

            let resource = Resource::new(vec![KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                self.service_name.clone(),
            )]);

            let provider = TracerProvider::builder()
                .with_batch_exporter(exporter, runtime::Tokio)
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(RandomIdGenerator::default())
                .with_resource(resource)
                .build();

            Some(provider)
        } else {
            None
        };

        // Set up Prometheus metrics if address is configured
        let prometheus_handle = if let Some(addr) = self.prometheus_addr {
            let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
            builder
                .with_http_listener(addr)
                .install()
                .map_err(|e| TelemetryError::PrometheusInit(e.to_string()))?;

            // Register metric descriptions
            register_metrics();

            Some(())
        } else {
            None
        };

        // Build the tracing subscriber
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let fmt_layer = tracing_subscriber::fmt::layer();

        // Build subscriber with optional OpenTelemetry layer
        if let Some(ref provider) = tracer_provider {
            let tracer = provider.tracer("durable");
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .with(otel_layer)
                .try_init()?;
        } else {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .try_init()?;
        }

        Ok(TelemetryHandle {
            tracer_provider,
            _prometheus_handle: prometheus_handle,
        })
    }
}

/// Handle for managing telemetry lifecycle.
///
/// Call `shutdown()` for graceful shutdown, which flushes pending spans and metrics.
pub struct TelemetryHandle {
    tracer_provider: Option<TracerProvider>,
    _prometheus_handle: Option<()>,
}

impl TelemetryHandle {
    /// Gracefully shut down telemetry, flushing any pending data.
    pub fn shutdown(self) {
        if let Some(provider) = self.tracer_provider
            && let Err(e) = provider.shutdown()
        {
            tracing::error!("Failed to shutdown tracer provider: {}", e);
        }
        // Prometheus handle is dropped automatically
    }
}
