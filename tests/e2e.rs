#[cfg(test)]
mod test {

    use datafusion::{
        assert_batches_eq,
        execution::SessionStateBuilder,
        logical_expr::ScalarUDF,
        physical_plan::displayable,
        prelude::{col, SessionContext},
    };
    use datafusion_proto::bytes::{
        logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
        physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
    };
    use std::sync::Arc;

    use ballista_python::{
        codec::{PyLogicalCodec, PyPhysicalCodec},
        factory::PythonFunctionFactory,
        setup_python_path,
        udf::PythonUDF,
    };

    #[tokio::test]
    async fn should_execute_python() -> datafusion::error::Result<()> {
        let ctx = context();

        let code = r#"
import pyarrow.compute as pc

conversation_rate_multiplier = 0.62137119

def to_miles(km_data):    
    return pc.multiply(km_data, conversation_rate_multiplier)    
"#;

        let udf = PythonUDF::from_code("to_miles", code).expect("udf created");
        let udf = ScalarUDF::from(udf);

        let df = ctx.sql("select unnest([1, 2, 3, 4, 5, 6, 7, 8, 9, 0]) as a").await?;
        let result = df.select(vec![col("a"), udf.call(vec![col("a")])])?.collect().await?;

        let expected = vec![
            "+---+--------------------+",
            "| a | to_miles(a)        |",
            "+---+--------------------+",
            "| 1 | 0.62137119         |",
            "| 2 | 1.24274238         |",
            "| 3 | 1.8641135699999998 |",
            "| 4 | 2.48548476         |",
            "| 5 | 3.10685595         |",
            "| 6 | 3.7282271399999996 |",
            "| 7 | 4.34959833         |",
            "| 8 | 4.97096952         |",
            "| 9 | 5.592340709999999  |",
            "| 0 | 0.0                |",
            "+---+--------------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_execute_python_sql() -> datafusion::error::Result<()> {
        let ctx = context();

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

        let df = ctx
            .sql("select a, to_miles(a) from (select unnest([1, 2, 3, 4, 5, 6, 7, 8, 9, 0]) as a)")
            .await?;
        let result = df.collect().await?;

        let expected = vec![
            "+---+--------------------+",
            "| a | to_miles(a)        |",
            "+---+--------------------+",
            "| 1 | 0.62137119         |",
            "| 2 | 1.24274238         |",
            "| 3 | 1.8641135699999998 |",
            "| 4 | 2.48548476         |",
            "| 5 | 3.10685595         |",
            "| 6 | 3.7282271399999996 |",
            "| 7 | 4.34959833         |",
            "| 8 | 4.97096952         |",
            "| 9 | 5.592340709999999  |",
            "| 0 | 0.0                |",
            "+---+--------------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_round_trip_logical_plan() -> datafusion::error::Result<()> {
        let ctx = context();
        let codec = PyLogicalCodec::default();

        let code = r#"
import pyarrow.compute as pc

conversation_rate_multiplier = 0.62137119

def to_miles(km_data):    
    return pc.multiply(km_data, conversation_rate_multiplier)    
"#;

        let udf = PythonUDF::from_code("to_miles", code).expect("udf created");
        let udf = ScalarUDF::from(udf);

        let df = ctx.sql("select unnest([1, 2, 3, 4, 5, 6, 7, 8, 9, 0]) as a").await?;
        let result = df.select(vec![col("a"), udf.call(vec![col("a")])])?;

        let plan = result.logical_plan();
        let bytes = logical_plan_to_bytes_with_extension_codec(&plan, &codec)?;
        let new_plan = logical_plan_from_bytes_with_extension_codec(&bytes, &ctx, &codec)?;

        assert_eq!(plan, &new_plan);

        Ok(())
    }

    #[tokio::test]
    async fn should_round_trip_physical_plan() -> datafusion::error::Result<()> {
        let ctx = context();
        let codec = PyPhysicalCodec::default();
        let code = r#"
import pyarrow.compute as pc

conversation_rate_multiplier = 0.62137119

def to_miles(km_data):    
    return pc.multiply(km_data, conversation_rate_multiplier)    
"#;

        let udf = PythonUDF::from_code("to_miles", code).expect("udf created");
        let udf = ScalarUDF::from(udf);

        let df = ctx.sql("select unnest([1, 2, 3, 4, 5, 6, 7, 8, 9, 0]) as a").await?;
        let result = df.select(vec![col("a"), udf.call(vec![col("a")])])?;

        let plan = result.create_physical_plan().await?;
        let bytes = physical_plan_to_bytes_with_extension_codec(plan.clone(), &codec)?;
        let new_plan = physical_plan_from_bytes_with_extension_codec(&bytes, &ctx, &codec)?;

        let plan_formatted = format!("{}", displayable(plan.as_ref()).indent(false));
        let new_plan_formatted = format!("{}", displayable(new_plan.as_ref()).indent(false));

        assert_eq!(plan_formatted, new_plan_formatted);

        Ok(())
    }

    fn context() -> SessionContext {
        setup_python_path().expect("python path to be set");
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_function_factory(Some(Arc::new(PythonFunctionFactory::default())))
            .build();

        SessionContext::new_with_state(state)
    }
}
