use crate::udf::PythonUDF;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::execution::context::{FunctionFactory, RegisterFunction};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{CreateFunction, Expr, ScalarUDF};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct PythonFunctionFactory {}

#[async_trait::async_trait]
impl FunctionFactory for PythonFunctionFactory {
    async fn create(
        &self,
        _state: &SessionState,
        statement: CreateFunction,
    ) -> datafusion::common::Result<RegisterFunction> {
        // TODO: check if language is python

        match statement.params.function_body {
            Some(Expr::Literal(ScalarValue::Utf8(Some(code)), _)) => {
                let name = statement.name;
                let return_type = statement.return_type.expect("return type expected");
                let argument_types = statement
                    .args
                    .map(|args| args.into_iter().map(|a| a.data_type).collect::<Vec<DataType>>())
                    .unwrap_or_default();
                let udf = PythonUDF::from_code_with_types(&name, &code, argument_types, return_type)?;
                let udf = ScalarUDF::from(udf);
                Ok(RegisterFunction::Scalar(Arc::new(udf)))
            }
            None => exec_err!("function definition to be provided")?,
            _ => exec_err!("invalid function definition provided")?,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::factory::PythonFunctionFactory;
    use datafusion::arrow::array::{ArrayRef, Float64Array, RecordBatch};
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[tokio::test]
    async fn basic_example() -> datafusion::common::Result<()> {
        crate::setup_python().expect("python environment to be set");

        let ctx = SessionContext::new().with_function_factory(Arc::new(PythonFunctionFactory::default()));

        let a: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]));
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)])?;

        ctx.register_batch("t", batch)?;

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

        let result = ctx.sql("select to_miles(a) from t").await?.collect().await?;

        let expected = ["+--------------------+",
            "| to_miles(t.a)      |",
            "+--------------------+",
            "| 0.62137119         |",
            "| 1.24274238         |",
            "| 1.8641135699999998 |",
            "| 2.48548476         |",
            "+--------------------+"];
        assert_batches_eq!(expected, &result);
        Ok(())
    }
}
