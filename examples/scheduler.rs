use ballista_core::error::BallistaError;
use ballista_python::codec::{PyLogicalCodec, PyPhysicalCodec};
use ballista_python::setup_python;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;
use std::net::AddrParseError;
use std::sync::Arc;

///
/// # Custom Ballista Scheduler
///
/// This example demonstrates how to crate custom ballista schedulers with support
/// for custom logical and physical codecs.
///
#[tokio::main]
async fn main() -> ballista_core::error::Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("ballista_python=debug,ballista_core=debug,ballista_executor=debug,ballista_scheduler=info")
        .is_test(true)
        .try_init();

    setup_python();

    let config: SchedulerConfig = SchedulerConfig {
        override_logical_codec: Some(Arc::new(PyLogicalCodec::default())),
        override_physical_codec: Some(Arc::new(PyPhysicalCodec::default())),
        ..Default::default()
    };

    let addr = format!("{}:{}", config.bind_host, config.bind_port);
    let addr = addr
        .parse()
        .map_err(|e: AddrParseError| BallistaError::Configuration(e.to_string()))?;

    let cluster = BallistaCluster::new_from_config(&config).await?;
    start_server(cluster, addr, Arc::new(config)).await?;

    Ok(())
}
