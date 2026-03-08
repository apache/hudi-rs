-- ============================================================================
-- Table: v9_txns_mor_nonpart_nometa
-- Type: MOR (Merge-on-Read)
-- Key Generation: Non-partitioned
-- Table Version: 9
-- Hudi Version: 1.1.1
-- ============================================================================
-- Features:
--   - Metadata table: DISABLED
--   - Record index: N/A
--   - Secondary indexes: N/A
--   - Partitioned: NO
--   - Primary key: txn_id
--
-- Operations demonstrated:
--   - INSERT, UPDATE, DELETE, COMPACTION, CLUSTERING
-- ============================================================================

CREATE TABLE v9_txns_mor_nonpart_nometa
(
    txn_id           STRING,
    account_id       STRING,
    txn_ts           BIGINT,
    txn_datetime     TIMESTAMP,
    txn_date         DATE,
    amount           DECIMAL(15,2),
    currency         STRING,
    txn_type         STRING,
    merchant_name    STRING,
    is_international BOOLEAN,
    fee_amount       DECIMAL(10,2),
    txn_metadata     STRING
) USING HUDI
TBLPROPERTIES (
    type = 'mor',
    primaryKey = 'txn_id',
    preCombineField = 'txn_ts',
    'hoodie.datasource.write.keygenerator.class' = 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
    'hoodie.metadata.enable' = 'false',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.clustering.inline' = 'false',
    'hoodie.clustering.async.enabled' = 'false',
    'hoodie.compact.inline' = 'false',
    'hoodie.compact.schedule.inline' = 'false'
);

-- ============================================================================
-- INSERT: Initial 8 records (no partitioning)
-- ============================================================================
INSERT INTO v9_txns_mor_nonpart_nometa VALUES
    ('TXN-001', 'ACC-A', 1700000000001, TIMESTAMP '2024-01-15 10:30:00', DATE '2024-01-15',
     1250.00, 'USD', 'debit', 'Amazon', false, 0.00,
     '{"category":"retail","mcc":"5411","risk_score":0.05}'),
    ('TXN-002', 'ACC-B', 1700000000002, TIMESTAMP '2024-01-15 11:45:00', DATE '2024-01-15',
     89.99, 'USD', 'debit', 'Netflix', false, 0.00,
     '{"category":"subscription","mcc":"4899","risk_score":0.02}'),
    ('TXN-003', 'ACC-A', 1700000000003, TIMESTAMP '2024-01-15 14:20:00', DATE '2024-01-15',
     5000.00, 'USD', 'transfer', NULL, false, 25.00,
     '{"category":"transfer","mcc":"4829","risk_score":0.15}'),
    ('TXN-004', 'ACC-C', 1700000000004, TIMESTAMP '2024-01-15 09:00:00', DATE '2024-01-15',
     450.75, 'EUR', 'debit', 'Zalando', false, 0.00,
     '{"category":"retail","mcc":"5651","risk_score":0.03}'),
    ('TXN-005', 'ACC-D', 1700000000005, TIMESTAMP '2024-01-15 16:30:00', DATE '2024-01-15',
     2100.00, 'GBP', 'credit', 'Salary Deposit', false, 0.00,
     '{"category":"income","mcc":"6011","risk_score":0.01}'),
    ('TXN-006', 'ACC-C', 1700000000006, TIMESTAMP '2024-01-15 18:45:00', DATE '2024-01-15',
     175.50, 'EUR', 'debit', 'Lufthansa', true, 3.50,
     '{"category":"travel","mcc":"3000","risk_score":0.08}'),
    ('TXN-007', 'ACC-E', 1700000000007, TIMESTAMP '2024-01-16 02:15:00', DATE '2024-01-16',
     8900.00, 'USD', 'debit', 'Singapore Airlines', true, 45.00,
     '{"category":"travel","mcc":"3000","risk_score":0.12}'),
    ('TXN-008', 'ACC-F', 1700000000008, TIMESTAMP '2024-01-16 08:00:00', DATE '2024-01-16',
     320.25, 'USD', 'debit', 'Grab', false, 0.00,
     '{"category":"transport","mcc":"4121","risk_score":0.04}');

-- ============================================================================
-- UPDATE: Modify txn_type for TXN-001 (mark as reversed)
-- ============================================================================
UPDATE v9_txns_mor_nonpart_nometa
SET txn_type = 'reversal', txn_ts = 1700100000001
WHERE txn_id = 'TXN-001';

-- ============================================================================
-- DELETE: Remove TXN-002
-- ============================================================================
DELETE FROM v9_txns_mor_nonpart_nometa WHERE txn_id = 'TXN-002';

-- ============================================================================
-- UPDATE: Modify amount for TXN-005
-- ============================================================================
UPDATE v9_txns_mor_nonpart_nometa
SET amount = 2500.00, txn_ts = 1700200000005
WHERE txn_id = 'TXN-005';

-- ============================================================================
-- DELETE: Remove TXN-005 (after update)
-- ============================================================================
DELETE FROM v9_txns_mor_nonpart_nometa WHERE txn_id = 'TXN-005';

-- ============================================================================
-- UPDATE: Modify fee_amount for TXN-007
-- ============================================================================
UPDATE v9_txns_mor_nonpart_nometa
SET fee_amount = 75.00, txn_ts = 1700300000007
WHERE txn_id = 'TXN-007';

-- ============================================================================
-- INSERT: Add 2 more records
-- ============================================================================
INSERT INTO v9_txns_mor_nonpart_nometa VALUES
    ('TXN-009', 'ACC-G', 1700400000009, TIMESTAMP '2024-01-17 10:00:00', DATE '2024-01-17',
     1500.00, 'EUR', 'debit', 'IKEA', false, 0.00,
     '{"category":"retail","mcc":"5712","risk_score":0.04}'),
    ('TXN-010', 'ACC-H', 1700400000010, TIMESTAMP '2024-01-17 11:30:00', DATE '2024-01-17',
     2200.00, 'EUR', 'transfer', NULL, false, 15.00,
     '{"category":"transfer","mcc":"4829","risk_score":0.10}');

-- ============================================================================
-- INSERT: Add more records to create file groups for compaction/clustering
-- ============================================================================
INSERT INTO v9_txns_mor_nonpart_nometa VALUES
    ('TXN-011', 'ACC-I', 1700500000011, TIMESTAMP '2024-01-18 09:00:00', DATE '2024-01-18',
     999.99, 'EUR', 'debit', 'MediaMarkt', false, 0.00,
     '{"category":"electronics","mcc":"5732","risk_score":0.06}'),
    ('TXN-012', 'ACC-J', 1700500000012, TIMESTAMP '2024-01-18 14:00:00', DATE '2024-01-18',
     350.00, 'GBP', 'debit', 'British Airways', true, 7.50,
     '{"category":"travel","mcc":"3000","risk_score":0.09}');

INSERT INTO v9_txns_mor_nonpart_nometa VALUES
    ('TXN-013', 'ACC-K', 1700600000013, TIMESTAMP '2024-01-19 08:00:00', DATE '2024-01-19',
     750.00, 'USD', 'debit', 'Best Buy', false, 0.00,
     '{"category":"electronics","mcc":"5732","risk_score":0.05}'),
    ('TXN-014', 'ACC-L', 1700600000014, TIMESTAMP '2024-01-19 09:30:00', DATE '2024-01-19',
     125.50, 'USD', 'debit', 'Uber', false, 0.00,
     '{"category":"transport","mcc":"4121","risk_score":0.03}');

-- ============================================================================
-- COMPACTION: Merge log files with base parquet files
-- ============================================================================
CALL run_compaction(op => 'schedule', table => 'v9_txns_mor_nonpart_nometa');

CALL run_compaction(op => 'run', table => 'v9_txns_mor_nonpart_nometa');

-- ============================================================================
-- CLUSTERING: Manually trigger clustering to reorganize files
-- ============================================================================
CALL run_clustering(table => 'v9_txns_mor_nonpart_nometa');

-- ============================================================================
-- INSERT: Add records after compaction and clustering
-- ============================================================================
INSERT INTO v9_txns_mor_nonpart_nometa VALUES
    ('TXN-015', 'ACC-M', 1700700000015, TIMESTAMP '2024-01-20 10:00:00', DATE '2024-01-20',
     4500.00, 'USD', 'debit', 'Japan Airlines', true, 50.00,
     '{"category":"travel","mcc":"3000","risk_score":0.11}'),
    ('TXN-016', 'ACC-N', 1700700000016, TIMESTAMP '2024-01-20 12:00:00', DATE '2024-01-20',
     88.00, 'USD', 'debit', 'GrabFood', false, 0.00,
     '{"category":"food","mcc":"5812","risk_score":0.02}');
