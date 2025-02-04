use pyo3::types::{PyAnyMethods, PyBytes, PyBytesMethods};
use pyo3::{PyObject, PyResult, Python};
use std::fmt::Debug;

static MODULE: &str = "cloudpickle";
static FUN_LOADS: &str = "loads";
static FUN_DUMPS: &str = "dumps";

/// Serde protocol for UD(a)F
#[derive(Debug)]
pub struct CloudPickle {
    loads: PyObject,
    dumps: PyObject,
}

impl CloudPickle {
    pub fn try_new(py: Python<'_>) -> PyResult<Self> {
        //let m = PyModule::new_bound(py, "example_udf").expect("module created");

        let module = py.import_bound(MODULE)?;
        //let register_module = module.getattr("register_pickle_by_value")?;
        //register_module.call1((m,))?;

        let loads = module.getattr(FUN_LOADS)?.unbind();
        let dumps = module.getattr(FUN_DUMPS)?.unbind();

        Ok(Self { loads, dumps })
    }

    pub fn pickle(&self, py: Python<'_>, py_any: &PyObject) -> PyResult<Vec<u8>> {
        let b: PyObject = self.dumps.call1(py, (py_any,))?.extract(py)?;
        let blob = b.downcast_bound::<PyBytes>(py)?.clone();

        Ok(blob.as_bytes().to_owned())
    }

    pub fn unpickle(&self, py: Python<'_>, blob: &[u8]) -> PyResult<PyObject> {
        let t: PyObject = self.loads.call1(py, (blob,))?.extract(py)?;

        Ok(t)
    }
}
