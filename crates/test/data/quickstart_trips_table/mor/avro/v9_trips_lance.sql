-- ============================================================================
-- Table: v9_trips_lance_mor
-- Type: MOR (Merge-on-Read) with Lance base file format
-- Table Version: 9
-- Hudi Version: 1.2.0-rc1
-- ============================================================================
-- Features:
--   - Base file format: LANCE (columnar format optimized for ML/AI workloads)
--   - Merge-on-Read: updates write to delta logs, compacted later
--   - Metadata table: ENABLED
--   - Record index: ENABLED
--   - Small file limit: 0 (disabled)
--   - Inline compaction: DISABLED
--
-- Operations demonstrated:
--   - INSERT (8 records across 3 partitions)
--   - UPDATE (modify fare for specific records, writes to delta log)
--   - DELETE (remove records)
-- ============================================================================

CREATE TABLE v9_trips_lance_mor
(
    ts     BIGINT,
    uuid   STRING,
    rider  STRING,
    driver STRING,
    fare   DOUBLE,
    city   STRING
) USING HUDI
PARTITIONED BY (city)
TBLPROPERTIES (
    type = 'mor',
    primaryKey = 'uuid',
    preCombineField = 'ts',
    'hoodie.table.base.file.format' = 'LANCE',
    'hoodie.datasource.write.record.merger.impls' = 'org.apache.hudi.DefaultSparkRecordMerger',
    'hoodie.metadata.enable' = 'true',
    'hoodie.metadata.record.index.enable' = 'true',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.compact.inline' = 'false',
    'hoodie.compact.inline.max.delta.commits' = '5'
);

-- ============================================================================
-- INSERT: Initial 8 records across 3 partitions (san_francisco, sao_paulo, chennai)
-- ============================================================================
INSERT INTO v9_trips_lance_mor VALUES
    (1695159649087, '334e26e9-8355-45cc-97c6-c31daf0df330', 'rider-A', 'driver-K', 19.10, 'san_francisco'),
    (1695091554788, 'e96c4396-3fad-413a-a942-4cb36106d721', 'rider-C', 'driver-M', 27.70, 'san_francisco'),
    (1695046462179, '9909a8b1-2d15-4d3d-8ec9-efc48c536a00', 'rider-D', 'driver-L', 33.90, 'san_francisco'),
    (1695332066204, '1dced545-862b-4ceb-8b43-d2a568f6616b', 'rider-E', 'driver-O', 93.50, 'san_francisco'),
    (1695516137016, 'e3cf430c-889d-4015-bc98-59bdce1e530c', 'rider-F', 'driver-P', 34.15, 'sao_paulo'),
    (1695376420876, '7a84095f-737f-40bc-b62f-6b69664712d2', 'rider-G', 'driver-Q', 43.40, 'sao_paulo'),
    (1695173887231, '3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04', 'rider-I', 'driver-S', 41.06, 'chennai'),
    (1695115999911, 'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa', 'rider-J', 'driver-T', 17.85, 'chennai');

-- ============================================================================
-- UPDATE: Modify fare for rider-A (writes to delta log)
-- ============================================================================
UPDATE v9_trips_lance_mor
SET fare = 0, ts = 1695200000000
WHERE rider = 'rider-A';

-- ============================================================================
-- UPDATE: Modify fare for rider-C (writes to delta log)
-- ============================================================================
UPDATE v9_trips_lance_mor
SET fare = 15.50, ts = 1695250000000
WHERE rider = 'rider-C';

-- ============================================================================
-- DELETE: Remove record for rider-F
-- ============================================================================
DELETE FROM v9_trips_lance_mor WHERE rider = 'rider-F';

-- ============================================================================
-- UPDATE: Modify fare for rider-G
-- ============================================================================
UPDATE v9_trips_lance_mor
SET fare = 0, ts = 1695400000000
WHERE rider = 'rider-G';

-- ============================================================================
-- DELETE: Remove record for rider-J
-- ============================================================================
DELETE FROM v9_trips_lance_mor WHERE rider = 'rider-J';

-- ============================================================================
-- INSERT: Add more records
-- ============================================================================
INSERT INTO v9_trips_lance_mor VALUES
    (1695800000001, 'c3d4e5f6-a7b8-9012-cdef-123456789012', 'rider-M', 'driver-W', 48.75, 'san_francisco'),
    (1695800000002, 'd4e5f6a7-b8c9-0123-def0-234567890123', 'rider-N', 'driver-X', 37.20, 'san_francisco'),
    (1695900000001, 'e5f6a7b8-c9d0-1234-ef01-345678901234', 'rider-O', 'driver-Y', 71.50, 'chennai'),
    (1695900000002, 'f6a7b8c9-d0e1-2345-f012-456789012345', 'rider-P', 'driver-Z', 29.80, 'chennai');
