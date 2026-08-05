-- ============================================================================
-- Table: v9_lance_nonpartitioned_cow
-- Type: COW (Copy-on-Write), non-partitioned, Lance base file format
-- Table Version: 9
-- Hudi Version: 1.2.0-rc1
-- ============================================================================
-- Features:
--   - Base file format: LANCE
--   - Non-partitioned table (common for ML feature stores)
--   - Record index: ENABLED
--
-- Operations demonstrated:
--   - INSERT (initial dataset)
--   - UPDATE (modify records)
--   - DELETE (remove records)
-- ============================================================================

CREATE TABLE v9_lance_nonpartitioned_cow
(
    id       INT,
    name     STRING,
    category STRING,
    score    DOUBLE,
    tags     ARRAY<STRING>,
    updated_at BIGINT
) USING HUDI
TBLPROPERTIES (
    type = 'cow',
    primaryKey = 'id',
    preCombineField = 'updated_at',
    'hoodie.table.base.file.format' = 'LANCE',
    'hoodie.datasource.write.record.merger.impls' = 'org.apache.hudi.DefaultSparkRecordMerger',
    'hoodie.metadata.enable' = 'true',
    'hoodie.metadata.record.index.enable' = 'true',
    'hoodie.parquet.small.file.limit' = '0'
);

-- ============================================================================
-- INSERT: Initial dataset (ML feature store style)
-- ============================================================================
INSERT INTO v9_lance_nonpartitioned_cow VALUES
    (1, 'feature-set-alpha', 'vision', 0.92, array('cnn', 'resnet', 'imagenet'), 1700000000000),
    (2, 'feature-set-beta',  'nlp',    0.87, array('bert', 'transformer'), 1700000000001),
    (3, 'feature-set-gamma', 'vision', 0.95, array('yolo', 'detection'), 1700000000002),
    (4, 'feature-set-delta', 'audio',  0.78, array('wav2vec', 'speech'), 1700000000003),
    (5, 'feature-set-epsilon', 'nlp',  0.91, array('gpt', 'generation'), 1700000000004),
    (6, 'feature-set-zeta',  'tabular', 0.89, array('xgboost', 'classification'), 1700000000005),
    (7, 'feature-set-eta',   'vision', 0.88, array('segmentation', 'unet'), 1700000000006),
    (8, 'feature-set-theta', 'audio',  0.82, array('whisper', 'transcription'), 1700000000007);

-- ============================================================================
-- UPDATE: Improve scores after retraining
-- ============================================================================
UPDATE v9_lance_nonpartitioned_cow
SET score = 0.96, updated_at = 1700100000000
WHERE id = 1;

UPDATE v9_lance_nonpartitioned_cow
SET score = 0.93, tags = array('bert', 'transformer', 'finetuned'), updated_at = 1700100000001
WHERE id = 2;

-- ============================================================================
-- DELETE: Deprecate old feature set
-- ============================================================================
DELETE FROM v9_lance_nonpartitioned_cow WHERE id = 4;

-- ============================================================================
-- INSERT: Add new feature sets
-- ============================================================================
INSERT INTO v9_lance_nonpartitioned_cow VALUES
    (9,  'feature-set-iota',  'multimodal', 0.94, array('clip', 'contrastive'), 1700200000000),
    (10, 'feature-set-kappa', 'vision',     0.97, array('sam', 'segmentation', 'foundation'), 1700200000001);
