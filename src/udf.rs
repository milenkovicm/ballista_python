use datafusion::arrow::array::{make_array, Array, ArrayData};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Signature;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Volatility};
use pyo3::types::{PyAnyMethods, PyModule, PyTuple};
use pyo3::{Py, PyAny, PyObject, PyResult, Python};
use std::any::Any;
use std::fmt::Debug;

/// Implements [`ScalarUDFImpl`] for functions that have a single signature and
/// return type.
pub struct PythonUDF {
    pub name: String,
    pub signature: Signature,
    pub input_types: Vec<DataType>,
    pub return_type: DataType,
    pub func: PyObject,
}

impl Debug for PythonUDF {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("PythonUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("input_types", &self.input_types)
            .field("return_type", &self.return_type)
            .field("func", &"<FUNC>")
            .finish()
    }
}

impl PythonUDF {
    /// Create a new `PythonUDF` from a name, input types, return type and
    /// implementation.
    pub fn new(
        name: impl Into<String>,
        input_types: Vec<DataType>,
        return_type: DataType,
        volatility: Volatility,
        func: PyObject,
    ) -> Self {
        Self::new_with_signature(
            name,
            Signature::exact(input_types.clone(), volatility),
            input_types,
            return_type,
            func,
        )
    }

    /// Create a new `SimpleScalarUDF` from a name, signature, return type and
    /// implementation.
    pub fn new_with_signature(
        name: impl Into<String>,
        signature: Signature,
        input_types: Vec<DataType>,
        return_type: DataType,

        func: PyObject,
    ) -> Self {
        log::debug!("PythonUDF::new() ...");
        Self {
            name: name.into(),
            signature,
            input_types,
            return_type,
            func,
        }
    }
    /// Function used for testing ONLY
    ///
    /// Please do read warnings at [PyModule::from_code_bound] to understand
    /// why this function is dangerous.
    pub fn from_code(name: &str, code: &str) -> Result<Self> {
        // TODO: we can add proper signature

        let py_function: PyResult<Py<PyAny>> = Python::with_gil(|py| {
            // we need to
            let udf_module =
            // TODO: we need better mutly file handling and module name
            PyModule::from_code_bound(py, code, "main.py", "__main__")?;

            // At the moment we assume that function will be named udf, that could be changed
            Ok(udf_module.getattr(name)?.unbind())
        });
        let function = PythonUDF::new(
            name,
            vec![DataType::Float64],
            DataType::Float64,
            Volatility::Volatile,
            py_function.expect("function to compile "),
        );

        Ok(function)
    }
}

impl ScalarUDFImpl for PythonUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        let array_refs = ColumnarValue::values_to_arrays(args)?;
        let array_data: Result<_> = Python::with_gil(|py| {
            // 1. cast args to PyArrow arrays
            let py_args = array_refs
                .iter()
                .map(|arg| {
                    arg.into_data()
                        .to_pyarrow(py)
                        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let py_args = PyTuple::new_bound(py, py_args);

            // 2. call function
            let value = self
                .func
                .call_bound(py, py_args, None)
                .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

            // 3. cast to arrow::array::Array
            ArrayData::from_pyarrow_bound(value.bind(py)).map_err(|e| DataFusionError::Execution(format!("{e:?}")))
        });

        Ok(make_array(array_data?).into())
    }
}
