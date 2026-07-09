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

use std::any::Any;

use datafusion::arrow::array::{Array, Int32Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;

const QUERY: &str = r#"
SELECT id
FROM t
WHERE a NOT IN (SELECT k FROM u)
"#;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    ctx.sql("CREATE TABLE t(id INT, a INT)")
        .await?
        .collect()
        .await?;
    ctx.sql("INSERT INTO t VALUES (1, 1), (2, 2)")
        .await?
        .collect()
        .await?;
    ctx.sql("CREATE TABLE u(k INT)").await?.collect().await?;
    ctx.sql("INSERT INTO u VALUES (NULL)")
        .await?
        .collect()
        .await?;

    let batches = ctx.sql(QUERY).await?.collect().await?;

    println!("NOT IN null-aware anti join SQL:\n{QUERY}");
    println!("DataFusion actual result:");
    println!("{}", pretty_format_batches(&batches)?);

    let actual_ids = collect_i32_column(&batches)?;
    let expected_ids: Vec<i32> = vec![];

    if actual_ids != expected_ids {
        return Err(DataFusionError::Execution(format!(
            "NOT IN null-aware semantics bug reproduced: expected no rows, got ids {actual_ids:?}"
        )));
    }

    Ok(())
}

fn collect_i32_column(batches: &[RecordBatch]) -> Result<Vec<i32>> {
    let mut values = vec![];
    for batch in batches {
        let column = batch.column(0);
        let array = as_int32_array(column.as_any())?;
        for row in 0..array.len() {
            if array.is_valid(row) {
                values.push(array.value(row));
            }
        }
    }
    Ok(values)
}

fn as_int32_array(array: &dyn Any) -> Result<&Int32Array> {
    array.downcast_ref::<Int32Array>().ok_or_else(|| {
        DataFusionError::Execution(
            "expected the first result column to be Int32".to_string(),
        )
    })
}
