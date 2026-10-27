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

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use object_store::buffered::BufWriter;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, parse_url_opts};
use parquet::arrow::ArrowWriter;
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow,
    RecordBatchIterator, RegionArrow, SupplierArrow,
};
use url::Url;

use crate::{collect_cloud_env_vars, is_cloud_url};

fn writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()
}

fn write_parquet_local(
    batches: &mut dyn Iterator<Item = RecordBatch>,
    schema: &SchemaRef,
    path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_props()))?;
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

async fn write_parquet_cloud(
    batches: &mut dyn Iterator<Item = RecordBatch>,
    schema: &SchemaRef,
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
) -> Result<(), Box<dyn std::error::Error>> {
    let buf_writer = BufWriter::new(Arc::clone(store), path.clone());
    let mut writer = AsyncArrowWriter::try_new(buf_writer, schema.clone(), Some(writer_props()))?;
    for batch in batches {
        writer.write(&batch).await?;
    }
    writer.close().await?;
    Ok(())
}

fn generate_table_local(
    name: &str,
    output_dir: &std::path::Path,
    batches: &mut dyn Iterator<Item = RecordBatch>,
    schema: &SchemaRef,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_dir = output_dir.join(name);
    std::fs::create_dir_all(&table_dir)?;
    println!("  Generating table: {name} ...");
    write_parquet_local(batches, schema, &table_dir.join("data.parquet"))?;
    println!("  Done: {name}");
    Ok(())
}

async fn generate_table_cloud(
    name: &str,
    store: &Arc<dyn ObjectStore>,
    base_path: &ObjectPath,
    batches: &mut dyn Iterator<Item = RecordBatch>,
    schema: &SchemaRef,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_path = ObjectPath::from(format!("{base_path}/{name}/data.parquet"));
    println!("  Generating table: {name} ...");
    write_parquet_cloud(batches, schema, store, &file_path).await?;
    println!("  Done: {name}");
    Ok(())
}

macro_rules! gen_all_tables {
    ($sf:expr, $gen_fn:ident $(, $ctx:expr)*) => {{
        let tables: Vec<(&str, Box<dyn FnOnce(f64) -> (Box<dyn Iterator<Item = RecordBatch>>, SchemaRef)>)> = vec![
            ("nation", Box::new(|sf| {
                let it = NationArrow::new(NationGenerator::new(sf, 1, 1));
                let s = it.schema().clone(); (Box::new(it) as Box<dyn Iterator<Item = RecordBatch>>, s)
            })),
            ("region", Box::new(|sf| {
                let it = RegionArrow::new(RegionGenerator::new(sf, 1, 1));
                let s = it.schema().clone(); (Box::new(it) as _, s)
            })),
            ("part", Box::new(|sf| {
                let it = PartArrow::new(PartGenerator::new(sf, 1, 1));
                let s = it.schema().clone(); (Box::new(it) as _, s)
            })),
            ("supplier", Box::new(|sf| {
                let it = SupplierArrow::new(SupplierGenerator::new(sf, 1, 1));
                let s = it.schema().clone(); (Box::new(it) as _, s)
            })),
            ("partsupp", Box::new(|sf| {
                let it = PartSuppArrow::new(PartSuppGenerator::new(sf, 1, 1));
                let s = it.schema().clone(); (Box::new(it) as _, s)
            })),
            ("customer", Box::new(|sf| {
                let it = CustomerArrow::new(CustomerGenerator::new(sf, 1, 1));
                let s = it.schema().clone(); (Box::new(it) as _, s)
            })),
            ("orders", Box::new(|sf| {
                let it = OrderArrow::new(OrderGenerator::new(sf, 1, 1));
                let s = it.schema().clone(); (Box::new(it) as _, s)
            })),
            ("lineitem", Box::new(|sf| {
                let it = LineItemArrow::new(LineItemGenerator::new(sf, 1, 1));
                let s = it.schema().clone(); (Box::new(it) as _, s)
            })),
        ];
        for (name, factory) in tables {
            let (mut batches, schema) = factory($sf);
            $gen_fn(name, &mut *batches, &schema $(, $ctx)*).await?;
        }
    }};
}

async fn do_generate_local(
    name: &str,
    batches: &mut dyn Iterator<Item = RecordBatch>,
    schema: &SchemaRef,
    output_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    generate_table_local(name, output_dir, batches, schema)
}

async fn do_generate_cloud(
    name: &str,
    batches: &mut dyn Iterator<Item = RecordBatch>,
    schema: &SchemaRef,
    ctx: &(Arc<dyn ObjectStore>, ObjectPath),
) -> Result<(), Box<dyn std::error::Error>> {
    generate_table_cloud(name, &ctx.0, &ctx.1, batches, schema).await
}

pub async fn run_generate(
    scale_factor: f64,
    output_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let sf = scale_factor;

    if is_cloud_url(output_dir) {
        println!("Generating TPC-H data (scale factor {sf}) to {output_dir}");

        let url = Url::parse(output_dir)?;
        let env_vars = collect_cloud_env_vars();
        let (store, object_path) = parse_url_opts(&url, env_vars)?;
        let ctx = (Arc::from(store) as Arc<dyn ObjectStore>, object_path);

        gen_all_tables!(sf, do_generate_cloud, &ctx);
    } else {
        let out_path = std::path::Path::new(output_dir);
        println!(
            "Generating TPC-H data (scale factor {sf}) into {}",
            out_path.display()
        );

        gen_all_tables!(sf, do_generate_local, out_path);
    }

    println!("All TPC-H tables generated successfully.");
    Ok(())
}
