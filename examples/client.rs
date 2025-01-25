use std::sync::Arc;

use ballista::prelude::*;
use ballista_python::{codec::PyLogicalCodec, setup_python_path, udf::PythonUDF};
use datafusion::{
    common::Result,
    execution::SessionStateBuilder,
    logical_expr::ScalarUDF,
    prelude::{col, lit, ParquetReadOptions, SessionConfig, SessionContext},
};
use pyo3::prelude::*;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results, using the DataFrame trait
#[tokio::main]
async fn main() -> Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("ballista_python=debug,ballista_core=debug,ballista_executor=debug,ballista_scheduler=debug")
        .is_test(true)
        .try_init();

    setup_python_path();

    let lc = Python::with_gil(|py| PyLogicalCodec::try_new(py).unwrap());

    let config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(lc))
        .with_target_partitions(4);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    let filename = format!("./data/alltypes.parquet");

    let code = r#"
import pyarrow.compute as pc
def udf(km_data):
    conversation_rate_multiplier = 0.62137119
    return pc.multiply(km_data, conversation_rate_multiplier)    
"#;

    let udf = PythonUDF::from_code("to_miles", code).expect("udf created");
    let udf = ScalarUDF::from(udf);

    ctx.read_parquet(filename, ParquetReadOptions::default())
        .await?
        .select(vec![udf.call(vec![lit(1.0) * col("id")])])?
        .show()
        .await?;

    Ok(())
}
