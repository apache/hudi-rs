use std::sync::Arc;

use datafusion::{
    error::Result,
    prelude::{DataFrame, SessionContext},
};
use hudi::HudiDataSource;

#[tokio::main]
async fn main() -> Result<()> {
    let data_path = "/your/hudi/data/path";

    let ctx = SessionContext::new();
    let hudi = HudiDataSource::new(data_path).await?;
    ctx.register_table("example", Arc::new(hudi))?;
    let df: DataFrame = ctx.sql("SELECT * FROM example").await?;
    df.show().await?;
    Ok(())
}
