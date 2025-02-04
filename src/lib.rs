use pyo3::{types::PyAnyMethods, Python};

/// custom codecs which knows how to serialize
/// python UDFs.
pub mod codec;
/// function factory handler, handles `CREATE FUNCTION` statements.
pub mod factory;
/// wrapper around python `cloudpickle` library
/// used to serialize python functions.
pub mod pickle;
/// datafusion (rust) UDF python function wrapper.
pub mod udf;

pub fn setup_python() -> pyo3::PyResult<()> {
    setup_python_path()?;
    assign_signal_check()?;
    Ok(())
}

/// setups python search path
// haven't put much effort to find the best way
// to setup python path
pub fn setup_python_path() -> pyo3::PyResult<()> {
    log::debug!("setting up python path ...");
    Python::with_gil(|py| -> pyo3::PyResult<()> {
        let version = py.version_info();
        let sys = py.import_bound("sys")?;
        let path = sys.getattr("path")?;
        path.call_method1(
            "append",
            (format!(
                ".venv/lib/python{}.{}/site-packages",
                version.major, version.minor
            ),),
        )?;
        Ok(())
    })
}

/// assign python signal check to shut down interpreter properly
// as described in the manual:
// https://pyo3.rs/v0.22.2/python-from-rust/calling-existing-code.html?#handling-system-signalsinterrupts-ctrl-c
pub fn assign_signal_check() -> pyo3::PyResult<()> {
    log::debug!("setting up python signal check...");
    Python::with_gil(|py| -> pyo3::PyResult<()> {
        let signal = py.import_bound("signal")?;
        // Set SIGINT to have the default action
        signal
            .getattr("signal")?
            .call1((signal.getattr("SIGINT")?, signal.getattr("SIG_DFL")?))?;
        Ok(())
    })
}
