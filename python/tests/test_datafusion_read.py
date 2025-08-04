from datafusion import SessionContext

from hudi import HudiDataSource


def test_datafusion_table_registry(get_sample_table):
    table_path = get_sample_table

    table = HudiDataSource(
        table_path, [("hoodie.read.use.read_optimized.mode", "true")]
    )
    ctx = SessionContext()
    ctx.register_table_provider("trips", table)
    df = ctx.sql("SELECT  city from trips order by city desc limit 1").to_arrow_table()
    assert df.to_pylist() == [{"city": "sao_paulo"}]
