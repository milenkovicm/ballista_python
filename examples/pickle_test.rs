use ballista_python::{pickle::CloudPickle, setup_python_path, udf::PythonUDF};
use datafusion::common::Result;
use pyo3::prelude::*;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results, using the DataFrame trait
#[tokio::main]
async fn main() -> Result<()> {
    setup_python_path();

    let code = r#"
import pyarrow.compute as pc
def udf(km_data):
    conversation_rate_multiplier = 0.62137119
    return pc.multiply(km_data, conversation_rate_multiplier)    
"#;

    let udf = PythonUDF::from_code("to_miles", code).expect("udf created");
    Python::with_gil(|py| {
        let c = CloudPickle::try_new(py).unwrap();
        let r = c.pickle(py, udf.py_func()).unwrap();
        let r = c.unpickle(py, &r[..]).unwrap();
        println!("{:?}", r);
    });

    Ok(())
}
