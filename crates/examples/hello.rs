use std::sync::Arc;
use std::env::var;

use datafusion::{
    error::Result,
    prelude::{DataFrame, SessionContext},
};
use hudi::HudiDataSource;

#[tokio::main]
async fn main() -> Result<()> {
    let data_path = var("HUDI_DATA_PATH").unwrap_or_else(|_| "/your/hudi/data/path".to_string());

    let ctx = SessionContext::new();
    let hudi = HudiDataSource::new(&data_path).await?;
    ctx.register_table("example", Arc::new(hudi))?;
    let df: DataFrame = ctx.sql("SELECT * FROM example").await?;
    df.show().await?;
    Ok(())
}
