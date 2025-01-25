# Python Distribution

Make [Datafusion Ballista](https://github.com/apache/datafusion-ballista) support python UDFs.

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
