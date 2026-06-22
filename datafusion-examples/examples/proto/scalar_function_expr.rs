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

//! See `main.rs` for how to run it.
//!
//! This example demonstrates the smallest useful round trip for a physical
//! [`ScalarFunctionExpr`]:
//!
//! 1. Build a physical expression for `sqrt(a)`.
//! 2. Serialize it to a protobuf `PhysicalExprNode`.
//! 3. Deserialize it back to a physical expression.
//! 4. Evaluate both expressions against the same batch.

use std::sync::Arc;

use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::Column;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::protobuf::physical_expr_node::ExprType;

pub async fn scalar_function_expr() -> Result<()> {
    println!("=== ScalarFunctionExpr Proto Round Trip Example ===\n");

    // 定义输入数据长什么样
    // 我们有一张输入表/输入 batch，它只有一列：
    // 列名: a
    // 类型: Float64
    // 是否允许 NULL: false
    // 也就是类似 SQL 里的：
    // CREATE TABLE t (
    //   a DOUBLE NOT NULL
    // );

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, false)]));

    // 对输入 batch 里的第 0 列 a 求平方根。
    let expr = Arc::new(ScalarFunctionExpr::try_new(
        datafusion::functions::math::sqrt(),
        vec![Arc::new(Column::new("a", 0))],
        schema.as_ref(),
        Arc::new(ConfigOptions::new()),
    )?) as Arc<dyn PhysicalExpr>;

    println!("Step 1: Built physical expression: {expr}");

    // 这段是在把刚刚造好的物理表达式 sqrt(a@0) 转成 proto 结构，序列化
    let codec = DefaultPhysicalExtensionCodec {};
    let proto = serialize_physical_expr(&expr, &codec)?;
    let Some(ExprType::ScalarUdf(scalar_udf)) = proto.expr_type.as_ref() else {
        return Err(DataFusionError::Execution(
            "Expected ScalarUdf proto node".to_string(),
        ));
    };

    println!(
        "Step 2: Serialized to proto: name={}, args={}, has_fun_definition={}",
        scalar_udf.name,
        scalar_udf.args.len(),
        scalar_udf.fun_definition.is_some()
    );

    // 反序列化
    let ctx = SessionContext::new();
    let decoded_expr = parse_physical_expr(&proto, &ctx.task_ctx(), &schema, &codec)?;

    println!("Step 3: Deserialized expression: {decoded_expr}");

    // 验证反序列化的表达式、执行结果是不是和原来一样
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Float64Array::from(vec![4.0, 9.0, 16.0]))],
    )?;

    let original = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
    let decoded = decoded_expr
        .evaluate(&batch)?
        .into_array(batch.num_rows())?;
    let original = original
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            DataFusionError::Execution("Expected Float64 result array".to_string())
        })?;
    let decoded = decoded
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            DataFusionError::Execution("Expected Float64 result array".to_string())
        })?;

    assert_eq!(original, decoded);

    println!("Step 4: Evaluated both expressions successfully");
    println!("  input:  [4.0, 9.0, 16.0]");
    println!("  output: {decoded:?}");

    Ok(())
}
