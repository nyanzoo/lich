#[cfg(feature = "otel")]
pub mod otel {
    #[macro_export]
    macro_rules! init_logger {
        () => {
            use logger::otel::create_logger;

            log::set_boxed_logger(create_logger()).unwrap();
            log::set_max_level(log::Level::Trace.to_level_filter());
        };
    }

    pub fn create_logger() -> Box<dyn log::Log> {
        use opentelemetry::KeyValue;
        use opentelemetry_appender_log::OpenTelemetryLogBridge;
        use opentelemetry_sdk::{
            logs::{Config, LoggerProvider},
            Resource,
        };
        use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
        use opentelemetry_stdout::LogExporterBuilder;

        let hostname = host();
        let exporter = LogExporterBuilder::default().build();
        let logger_provider = LoggerProvider::builder()
            .with_config(
                Config::default()
                    .with_resource(Resource::new(vec![KeyValue::new(SERVICE_NAME, hostname)])),
            )
            .with_simple_exporter(exporter)
            .build();

        let otel_log_appender = OpenTelemetryLogBridge::new(&logger_provider);
        Box::new(otel_log_appender)
    }

    fn host() -> String {
        std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string())
    }
}

#[cfg(feature = "simple")]
pub mod simple {

    #[macro_export]
    macro_rules! init_logger {
        () => {
            use env_logger;

            env_logger::init();
        };
    }
}

#[cfg(feature = "syslog")]
pub mod syslog {

    #[macro_export]
    macro_rules! init_logger {
        () => {
            use logger::syslog::create_logger;

            create_logger();
        };
    }

    pub fn create_logger() {
        use log::LevelFilter;
        use syslog::Facility;

        syslog::init(Facility::LOG_USER, LevelFilter::Trace, None).expect("init logger");
    }
}

#[cfg(feature = "otel")]
pub use otel::*;

#[cfg(feature = "syslog")]
pub use syslog::*;
