// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use pyo3::{types::PyAnyMethods, Python};

pub mod codec;
pub mod pickle;
pub mod udf;

pub fn setup_python_path() {
    Python::with_gil(|py| {
        let version = py.version_info();
        let sys = py.import_bound("sys").expect("sys");
        let path = sys.getattr("path").expect("path");
        path.call_method1(
            "append",
            (format!(
                ".venv/lib/python{}.{}/site-packages",
                version.major, version.minor
            ),),
        )
        .expect("set");
    });
}
