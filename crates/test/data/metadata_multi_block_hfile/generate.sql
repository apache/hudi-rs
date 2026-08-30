CREATE TABLE mdt_rich5 (
    ts     BIGINT, uuid STRING, rider STRING, driver STRING, fare DOUBLE, city STRING
) USING HUDI
PARTITIONED BY (city)
TBLPROPERTIES (
    type = 'mor', primaryKey = 'uuid', preCombineField = 'ts',
    'hoodie.metadata.enable' = 'true',
    'hoodie.metadata.record.index.enable' = 'true',
    'hoodie.metadata.index.column.stats.enable' = 'true',
    'hoodie.metadata.index.bloom.filter.enable' = 'true',
    -- Compact the metadata table after 2 delta commits rather than the default,
    -- so its records land in HFiles inside a fixture-sized job.
    'hoodie.metadata.compact.max.delta.commits' = '2',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.metadata.record.level.index.min.filegroup.count' = '1',
    'hoodie.metadata.record.level.index.max.filegroup.count' = '1',
    'hoodie.metadata.index.column.stats.file.group.count' = '1',
    'hoodie.metadata.index.bloom.filter.file.group.count' = '1'
);

INSERT INTO mdt_rich5 SELECT 1700000000000 + id,
  concat('uuid-', lpad(cast(id AS STRING), 8, '0')),
  concat('rider-', cast(id % 997 AS STRING)),
  concat('driver-', cast(id % 991 AS STRING)),
  cast(id % 500 AS DOUBLE) + 0.5,
  CASE cast(id % 4 AS INT) WHEN 0 THEN 'san_francisco' WHEN 1 THEN 'chennai'
                           WHEN 2 THEN 'sao_paulo' ELSE 'amsterdam' END
FROM range(0, 300000);

INSERT INTO mdt_rich5 SELECT 1700000000000 + id,
  concat('uuid-', lpad(cast(id AS STRING), 8, '0')),
  concat('rider-', cast(id % 997 AS STRING)),
  concat('driver-', cast(id % 991 AS STRING)),
  cast(id % 500 AS DOUBLE) + 0.5,
  CASE cast(id % 4 AS INT) WHEN 0 THEN 'san_francisco' WHEN 1 THEN 'chennai'
                           WHEN 2 THEN 'sao_paulo' ELSE 'amsterdam' END
FROM range(300000, 600000);

-- Updates that repeat keys from commit 1: what ENG-47731 needs for a merge to
-- actually happen, and what the old fixture never has.
UPDATE mdt_rich5 SET fare = fare + 1000.0, ts = ts + 1 WHERE uuid LIKE 'uuid-000001%';
UPDATE mdt_rich5 SET rider = 'rider-updated', ts = ts + 2 WHERE uuid LIKE 'uuid-000002%';
DELETE FROM mdt_rich5 WHERE uuid LIKE 'uuid-000003%';
