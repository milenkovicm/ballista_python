use std::sync::Arc;

use ballista::prelude::*;
use ballista_python::{codec::PyLogicalCodec, setup_python, udf::PythonUDF};
use datafusion::{
    common::Result,
    execution::SessionStateBuilder,
    logical_expr::ScalarUDF,
    prelude::{col, ParquetReadOptions, SessionConfig, SessionContext},
};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("ballista_python=debug,ballista_core=debug,ballista_executor=debug,ballista_scheduler=debug")
        .is_test(true)
        .try_init();

    setup_python().expect("python environment to be set");

    let config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(PyLogicalCodec::default()))
        .with_target_partitions(4);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    let code = r#"
import pyarrow.compute as pc

conversation_rate_multiplier = 0.62137119

def to_miles(km_data):    
    return pc.multiply(km_data, conversation_rate_multiplier)    
"#;

    let udf = PythonUDF::from_code("to_miles", code).expect("udf created");
    let udf = ScalarUDF::from(udf);

    ctx.read_parquet("./data/alltypes.parquet", ParquetReadOptions::default())
        .await?
        .select(vec![col("double_col"), udf.call(vec![col("double_col")])])?
        .show()
        .await?;

    Ok(())
}
