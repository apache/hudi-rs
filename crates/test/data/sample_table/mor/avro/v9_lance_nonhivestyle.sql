-- ============================================================================
-- Table: v9_lance_nonhivestyle_mor
-- Type: MOR (Merge-on-Read) with Lance and non-hive-style partitioning
-- Table Version: 9
-- Hudi Version: 1.2.0-rc1
-- ============================================================================
-- Features:
--   - Base file format: LANCE
--   - Non-hive-style partitioning (partition path without key=value format)
--   - MOR table type (updates go to delta logs)
--   - Record index: ENABLED
--
-- Operations demonstrated:
--   - INSERT (event log data)
--   - UPDATE (enrich events with additional info)
--   - DELETE (GDPR-style removal)
-- ============================================================================

CREATE TABLE v9_lance_nonhivestyle_mor
(
    event_id    STRING,
    user_id     STRING,
    event_type  STRING,
    payload     STRING,
    event_ts    BIGINT,
    event_date  STRING
) USING HUDI
PARTITIONED BY (event_date)
TBLPROPERTIES (
    type = 'mor',
    primaryKey = 'event_id',
    preCombineField = 'event_ts',
    'hoodie.table.base.file.format' = 'LANCE',
    'hoodie.datasource.write.record.merger.impls' = 'org.apache.hudi.DefaultSparkRecordMerger',
    'hoodie.metadata.enable' = 'true',
    'hoodie.metadata.record.index.enable' = 'true',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.datasource.write.hive_style_partitioning' = 'false',
    'hoodie.compact.inline' = 'false'
);

-- ============================================================================
-- INSERT: User activity events across multiple days
-- ============================================================================
INSERT INTO v9_lance_nonhivestyle_mor VALUES
    ('evt-001', 'user-100', 'page_view',  '{"page": "/home"}', 1700000000000, '2023-11-14'),
    ('evt-002', 'user-100', 'click',      '{"button": "signup"}', 1700000060000, '2023-11-14'),
    ('evt-003', 'user-101', 'page_view',  '{"page": "/pricing"}', 1700000120000, '2023-11-14'),
    ('evt-004', 'user-102', 'purchase',   '{"item": "pro-plan", "amount": 49.99}', 1700000180000, '2023-11-14'),
    ('evt-005', 'user-100', 'page_view',  '{"page": "/docs"}', 1700086400000, '2023-11-15'),
    ('evt-006', 'user-101', 'click',      '{"button": "download"}', 1700086460000, '2023-11-15'),
    ('evt-007', 'user-103', 'signup',     '{"method": "google"}', 1700086520000, '2023-11-15'),
    ('evt-008', 'user-103', 'page_view',  '{"page": "/onboarding"}', 1700086580000, '2023-11-15');

-- ============================================================================
-- INSERT: More events on a third day
-- ============================================================================
INSERT INTO v9_lance_nonhivestyle_mor VALUES
    ('evt-009', 'user-100', 'purchase',   '{"item": "enterprise", "amount": 199.99}', 1700172800000, '2023-11-16'),
    ('evt-010', 'user-102', 'page_view',  '{"page": "/settings"}', 1700172860000, '2023-11-16'),
    ('evt-011', 'user-101', 'purchase',   '{"item": "team-plan", "amount": 99.99}', 1700172920000, '2023-11-16'),
    ('evt-012', 'user-104', 'signup',     '{"method": "email"}', 1700172980000, '2023-11-16');

-- ============================================================================
-- UPDATE: Enrich events with session info (writes to delta log in MOR)
-- ============================================================================
UPDATE v9_lance_nonhivestyle_mor
SET payload = '{"page": "/home", "session": "sess-abc123"}', event_ts = 1700000000001
WHERE event_id = 'evt-001';

UPDATE v9_lance_nonhivestyle_mor
SET payload = '{"button": "signup", "session": "sess-abc123"}', event_ts = 1700000060001
WHERE event_id = 'evt-002';

-- ============================================================================
-- DELETE: GDPR removal - delete all events for user-103
-- ============================================================================
DELETE FROM v9_lance_nonhivestyle_mor WHERE user_id = 'user-103';

-- ============================================================================
-- INSERT: Backfill corrected events
-- ============================================================================
INSERT INTO v9_lance_nonhivestyle_mor VALUES
    ('evt-013', 'user-100', 'feature_use', '{"feature": "dashboard"}', 1700259200000, '2023-11-17'),
    ('evt-014', 'user-101', 'feature_use', '{"feature": "reports"}', 1700259260000, '2023-11-17');
