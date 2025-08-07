use ballista_python::{pickle::CloudPickle, setup_python, udf::PythonUDF};
use datafusion::common::Result;
use pyo3::{prelude::*, types::PyString};

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results, using the DataFrame trait

#[tokio::test]
async fn round_trip() -> Result<()> {
    setup_python().expect("python environment to be set");

    let code = r#"
import pyarrow.compute as pc

conversation_rate_multiplier = 0.62137119

def to_miles(km_data):    
    return pc.multiply(km_data, conversation_rate_multiplier)    
"#;

    let udf = PythonUDF::from_code("to_miles", code).expect("udf created");
    Python::with_gil(|py| {
        let c = CloudPickle::try_new(py).unwrap();
        let blob = c.pickle(py, &udf.func).unwrap();
        let unpickled = c.unpickle(py, &blob[..]).unwrap();
        let name = unpickled.getattr(py, "__name__").unwrap();
        let name = name.downcast_bound::<PyString>(py).unwrap().to_str().unwrap();

        assert_eq!("to_miles", name);
    });

    Ok(())
}
