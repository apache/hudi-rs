CREATE TABLE mdt_merge8 (
    ts BIGINT, uuid STRING, rider STRING, driver STRING, fare DOUBLE, city STRING
) USING HUDI
PARTITIONED BY (city)
TBLPROPERTIES (
    type = 'mor', primaryKey = 'uuid', preCombineField = 'ts',
    'hoodie.metadata.enable' = 'true',
    'hoodie.metadata.record.index.enable' = 'true',
    -- column_stats OFF on purpose. isShouldScanColStatsForTightBound returns
    -- COLUMN_STATS.isMetadataPartitionAvailable, so with it off every
    -- partition-stats record is written non-tight-bound, which is the only
    -- state in which mergeColumnStatsRecords widens bounds and sums counters.
    'hoodie.metadata.index.column.stats.enable' = 'false',
    'hoodie.metadata.index.partition.stats.enable' = 'true',
    'hoodie.metadata.index.bloom.filter.enable' = 'true',
    'hoodie.metadata.compact.max.delta.commits' = '2',
    'hoodie.compact.inline' = 'false',
    'hoodie.metadata.record.level.index.min.filegroup.count' = '1',
    'hoodie.metadata.record.level.index.max.filegroup.count' = '1',
    'hoodie.metadata.index.partition.stats.file.group.count' = '1',
    'hoodie.metadata.index.bloom.filter.file.group.count' = '1'
);

INSERT INTO mdt_merge8 SELECT 1700000000000 + id,
  concat('uuid-', lpad(cast(id AS STRING), 6, '0')),
  concat('rider-', cast(id % 97 AS STRING)),
  concat('driver-', cast(id % 91 AS STRING)),
  cast(id % 500 AS DOUBLE) + 0.5,
  CASE cast(id % 2 AS INT) WHEN 0 THEN 'san_francisco' ELSE 'chennai' END
FROM range(0, 4000);

-- Each update widens a bound past the previous extreme, so a merge that keeps
-- the newer record instead of widening produces a different number.
UPDATE mdt_merge8 SET fare = fare + 5000.0, ts = ts + 1 WHERE uuid LIKE 'uuid-0001%';
UPDATE mdt_merge8 SET fare = fare - 5000.0, ts = ts + 2 WHERE uuid LIKE 'uuid-0002%';
UPDATE mdt_merge8 SET rider = 'zzz-last',   ts = ts + 3 WHERE uuid LIKE 'uuid-0003%';
DELETE FROM mdt_merge8 WHERE uuid LIKE 'uuid-00000%';

-- A replacecommit: its deleteFileList is what drives stats records with
-- isDeleted=true.
INSERT OVERWRITE mdt_merge8 SELECT 1700000000000 + id,
  concat('uuid-', lpad(cast(id AS STRING), 6, '0')),
  concat('rider-', cast(id % 97 AS STRING)),
  concat('driver-', cast(id % 91 AS STRING)),
  cast(id % 500 AS DOUBLE) + 7777.5,
  CASE cast(id % 2 AS INT) WHEN 0 THEN 'san_francisco' ELSE 'chennai' END
FROM range(0, 2000);
