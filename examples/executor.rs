use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};
use ballista_python::codec::PyPhysicalCodec;
use std::sync::Arc;
///
/// # Custom Ballista Executor
///
/// This example demonstrates how to crate custom ballista executors with support
/// for custom logical and physical codecs.
///
#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("ballista_python=debug,ballista_core=debug,ballista_executor=debug,ballista_scheduler=debug")
        .is_test(true)
        .try_init();

    ballista_python::setup_python().expect("python environment to be set");

    let config: ExecutorProcessConfig = ExecutorProcessConfig {
        // logical codec is not needed at the executor
        // override_logical_codec: Some(Arc::new(PyLogicalCodec::default())),
        override_physical_codec: Some(Arc::new(PyPhysicalCodec::default())),
        ..Default::default()
    };

    start_executor_process(Arc::new(config)).await
}
