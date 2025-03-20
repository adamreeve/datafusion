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

use std::sync::Arc;
use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use futures::StreamExt;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::SessionContext;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::file::properties::WriterPropertiesBuilder;
use tempfile::TempDir;
use datafusion::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::physical_plan::execute_stream;

/// This example demonstrates reading and writing Parquet files that
/// are encrypted using Parquet Modular Encryption.

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let tmpdir = TempDir::new()?;
    println!("Test writing");
    write_encrypted(&tmpdir).await?;
    println!("Test reading");
    read_encrypted(&tmpdir).await?;
    Ok(())
}

async fn write_encrypted(tmpdir: &TempDir) -> datafusion::common::Result<()> {
    // Write an encrypted file
    let ctx = SessionContext::new();

    let a: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let b: ArrayRef = Arc::new(Int32Array::from(vec![1, 10, 10, 100]));
    let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)])?;

    // declare a table in memory. In Apache Spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;
    let df = ctx.table("t").await?;

    let mut table_options = TableParquetOptions::new();
    table_options.writer_configuration = Some(Arc::new(configure_encryption));

    df.write_parquet(
            tmpdir.path().to_str().unwrap(),
            DataFrameWriteOptions::new(),
            Some(table_options),
        )
        .await?;

    println!("Encrypted Parquet written to {:?}", tmpdir.path());

    Ok(())
}

async fn read_encrypted(tmpdir: &TempDir) -> datafusion::common::Result<()> {
    // Read an encrypted file from the parquet-testing repository
    let ctx = SessionContext::new();

    // Configure listing options
    let mut parquet_options = TableParquetOptions::new();
    parquet_options.read_configuration = Some(Arc::new(configure_decryption));
    let file_format = ParquetFormat::default().with_options(parquet_options).with_enable_pruning(true);
    let listing_options = ListingOptions::new(Arc::new(file_format));

    let file_name = std::fs::read_dir(tmpdir)?.next().unwrap()?;
    let table_path = format!("file://{}", file_name.path().as_os_str().to_str().unwrap());

    let _ = ctx
        .register_listing_table(
            "my_table",
            &table_path,
            listing_options.clone(),
            None,
            None,
        )
        .await?;

    let df = ctx.sql("SELECT * FROM my_table").await?;
    let plan = df.create_physical_plan().await?;

    let mut batch_stream = execute_stream(plan.clone(), ctx.task_ctx())?;
    println!("Reading stream");
    while let Some(batch) = batch_stream.next().await {
        println!("Batch rows: {}", batch?.num_rows());
    }
    println!("Done reading");
    Ok(())
}

fn configure_encryption(builder: WriterPropertiesBuilder) -> WriterPropertiesBuilder {
    let footer_key = b"0123456789012345".into();
    let encryption_properties = FileEncryptionProperties::builder(footer_key).build();
    builder.with_file_encryption_properties(encryption_properties)
}

fn configure_decryption(options: ArrowReaderOptions) -> ArrowReaderOptions {
    let footer_key = b"0123456789012345".into();
    let decryption_properties = FileDecryptionProperties::builder(footer_key).build().unwrap();
    options.with_file_decryption_properties(decryption_properties)
}
