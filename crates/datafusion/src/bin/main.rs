use std::sync::Arc;

use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};

use hudi_datafusion::HudiDataSource;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let hudi = HudiDataSource::new("/tmp/trips_table");
    ctx.register_table("trips_table", Arc::new(hudi))?;
    let df: DataFrame = ctx.sql("SELECT * from trips_table where fare > 20.0").await?;
    df.show().await?;
    Ok(())
}
