use std::sync::Arc;

use ballista::prelude::*;
use ballista_python::{codec::PyLogicalCodec, factory::PythonFunctionFactory, setup_python};
use datafusion::{
    common::Result,
    execution::SessionStateBuilder,
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
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

    let ctx = SessionContext::remote_with_state("df://localhost:50050", state)
        .await?
        .with_function_factory(Arc::new(PythonFunctionFactory::default()));

    let sql = r#"
CREATE FUNCTION to_miles(DOUBLE)
RETURNS DOUBLE
LANGUAGE PYTHON
AS '
import pyarrow.compute as pc
conversation_rate_multiplier = 0.62137119
def to_miles(km_data):
    return pc.multiply(km_data, conversation_rate_multiplier)
'
"#;

    ctx.sql(sql).await?.show().await?;

    ctx.register_parquet("t", "./data/alltypes.parquet", ParquetReadOptions::default())
        .await?;

    ctx.sql("select double_col, to_miles(double_col) from t")
        .await?
        .show()
        .await?;

    Ok(())
}
