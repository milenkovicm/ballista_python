# Python Distribution

Make [Datafusion Ballista](https://github.com/apache/datafusion-ballista) support python UDFs.

Also make case for: <https://github.com/apache/datafusion-python/pull/1003>

Setup python environment:

```bash
pyenv local 3.12
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

Run: [scheduler](examples/scheduler.rs), [executor](examples/executor.rs) and [client](examples/client.rs)

Client should return:

```text
+-----------------------------------+
| to_miles(Float64(1) * ?table?.id) |
+-----------------------------------+
| 2.48548476                        |
| 3.10685595                        |
| 3.7282271399999996                |
| 4.34959833                        |
| 1.24274238                        |
| 1.8641135699999998                |
| 0.0                               |
| 0.62137119                        |
+-----------------------------------+
```

## Datafusion Python

Created scheduler and executor works with patched datafusion-python as well.

A simple script will execute on ballista cluster:

```python
from datafusion import SessionContext
from datafusion import udf, functions as f
import pyarrow.compute as pc
import pyarrow

# SessionContext with url specified will connect to ballista cluster
ctx = SessionContext(url = "df://localhost:50050")

df = ctx.read_parquet("/Users/marko/TMP/yellow_tripdata_2021-01.parquet").aggregate(
    [f.col("passenger_count")], [f.count_star()]
)
df.show()


def to_miles(km_data):
    conversation_rate_multiplier = 0.62137119
    return pc.multiply(km_data, conversation_rate_multiplier)    

to_miles_udf = udf(to_miles, [pyarrow.float64()], pyarrow.float64(), "stable")

# its incorrect to convert passenger_count to miles
df = df.select(to_miles_udf(f.col("passenger_count")), f.col("passenger_count"))
df.show()
```

the proof of concept is available at [patched branch](https://github.com/milenkovicm/datafusion-python/tree/poc_ballista_support).

Note: if notebook complains about `cloudpickle` please `!pip install` it, did not have time to find out how to specify it as a dependency.
