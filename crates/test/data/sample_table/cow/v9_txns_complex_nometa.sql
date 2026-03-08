-- ============================================================================
-- Table: v9_txns_cow_complex_nometa
-- Type: COW (Copy-on-Write)
-- Key Generation: Complex (composite primary key)
-- Table Version: 9
-- Hudi Version: 1.1.1
-- ============================================================================
-- Features:
--   - Metadata table: DISABLED
--   - Record index: N/A
--   - Secondary indexes: N/A
--   - Partitioned: YES (by region)
--   - Primary key: txn_id, account_id (composite)
--
-- Operations demonstrated:
--   - INSERT, UPDATE, DELETE, INSERT OVERWRITE, CLUSTERING
-- ============================================================================

CREATE TABLE v9_txns_cow_complex_nometa
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
    txn_metadata     STRING,
    region           STRING
) USING HUDI
PARTITIONED BY (region)
TBLPROPERTIES (
    type = 'cow',
    primaryKey = 'txn_id,account_id',
    preCombineField = 'txn_ts',
    'hoodie.metadata.enable' = 'false',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.clustering.inline' = 'false',
    'hoodie.clustering.async.enabled' = 'false'
);

-- ============================================================================
-- INSERT: Initial 8 records across 3 partitions (us, eu, apac)
-- ============================================================================
INSERT INTO v9_txns_cow_complex_nometa VALUES
    ('TXN-001', 'ACC-A', 1700000000001, TIMESTAMP '2024-01-15 10:30:00', DATE '2024-01-15',
     1250.00, 'USD', 'debit', 'Amazon', false, 0.00,
     '{"category":"retail","mcc":"5411","risk_score":0.05}', 'us'),
    ('TXN-002', 'ACC-B', 1700000000002, TIMESTAMP '2024-01-15 11:45:00', DATE '2024-01-15',
     89.99, 'USD', 'debit', 'Netflix', false, 0.00,
     '{"category":"subscription","mcc":"4899","risk_score":0.02}', 'us'),
    ('TXN-003', 'ACC-A', 1700000000003, TIMESTAMP '2024-01-15 14:20:00', DATE '2024-01-15',
     5000.00, 'USD', 'transfer', NULL, false, 25.00,
     '{"category":"transfer","mcc":"4829","risk_score":0.15}', 'us'),
    ('TXN-004', 'ACC-C', 1700000000004, TIMESTAMP '2024-01-15 09:00:00', DATE '2024-01-15',
     450.75, 'EUR', 'debit', 'Zalando', false, 0.00,
     '{"category":"retail","mcc":"5651","risk_score":0.03}', 'eu'),
    ('TXN-005', 'ACC-D', 1700000000005, TIMESTAMP '2024-01-15 16:30:00', DATE '2024-01-15',
     2100.00, 'GBP', 'credit', 'Salary Deposit', false, 0.00,
     '{"category":"income","mcc":"6011","risk_score":0.01}', 'eu'),
    ('TXN-006', 'ACC-C', 1700000000006, TIMESTAMP '2024-01-15 18:45:00', DATE '2024-01-15',
     175.50, 'EUR', 'debit', 'Lufthansa', true, 3.50,
     '{"category":"travel","mcc":"3000","risk_score":0.08}', 'eu'),
    ('TXN-007', 'ACC-E', 1700000000007, TIMESTAMP '2024-01-16 02:15:00', DATE '2024-01-16',
     8900.00, 'USD', 'debit', 'Singapore Airlines', true, 45.00,
     '{"category":"travel","mcc":"3000","risk_score":0.12}', 'apac'),
    ('TXN-008', 'ACC-F', 1700000000008, TIMESTAMP '2024-01-16 08:00:00', DATE '2024-01-16',
     320.25, 'USD', 'debit', 'Grab', false, 0.00,
     '{"category":"transport","mcc":"4121","risk_score":0.04}', 'apac');

-- ============================================================================
-- UPDATE: Modify txn_type for TXN-001/ACC-A (mark as reversed)
-- Note: Complex key requires both columns in WHERE clause for efficient lookup
-- ============================================================================
UPDATE v9_txns_cow_complex_nometa
SET txn_type = 'reversal', txn_ts = 1700100000001
WHERE txn_id = 'TXN-001' AND account_id = 'ACC-A';

-- ============================================================================
-- DELETE: Remove TXN-002/ACC-B
-- ============================================================================
DELETE FROM v9_txns_cow_complex_nometa WHERE txn_id = 'TXN-002' AND account_id = 'ACC-B';

-- ============================================================================
-- UPDATE: Modify amount for TXN-005/ACC-D
-- ============================================================================
UPDATE v9_txns_cow_complex_nometa
SET amount = 2500.00, txn_ts = 1700200000005
WHERE txn_id = 'TXN-005' AND account_id = 'ACC-D';

-- ============================================================================
-- DELETE: Remove TXN-005/ACC-D (after update)
-- ============================================================================
DELETE FROM v9_txns_cow_complex_nometa WHERE txn_id = 'TXN-005' AND account_id = 'ACC-D';

-- ============================================================================
-- UPDATE: Modify fee_amount for TXN-007/ACC-E
-- ============================================================================
UPDATE v9_txns_cow_complex_nometa
SET fee_amount = 75.00, txn_ts = 1700300000007
WHERE txn_id = 'TXN-007' AND account_id = 'ACC-E';

-- ============================================================================
-- INSERT: Add 2 more records to eu partition
-- ============================================================================
INSERT INTO v9_txns_cow_complex_nometa VALUES
    ('TXN-009', 'ACC-G', 1700400000009, TIMESTAMP '2024-01-17 10:00:00', DATE '2024-01-17',
     1500.00, 'EUR', 'debit', 'IKEA', false, 0.00,
     '{"category":"retail","mcc":"5712","risk_score":0.04}', 'eu'),
    ('TXN-010', 'ACC-H', 1700400000010, TIMESTAMP '2024-01-17 11:30:00', DATE '2024-01-17',
     2200.00, 'EUR', 'transfer', NULL, false, 15.00,
     '{"category":"transfer","mcc":"4829","risk_score":0.10}', 'eu');

-- ============================================================================
-- INSERT OVERWRITE: Replace all data in eu partition
-- ============================================================================
INSERT OVERWRITE v9_txns_cow_complex_nometa PARTITION (region = 'eu')
SELECT txn_id, account_id, txn_ts, txn_datetime, txn_date, amount, currency,
       txn_type, merchant_name, is_international, fee_amount, txn_metadata
FROM (
    SELECT 'TXN-011' AS txn_id, 'ACC-I' AS account_id, 1700500000011 AS txn_ts,
           TIMESTAMP '2024-01-18 09:00:00' AS txn_datetime, DATE '2024-01-18' AS txn_date,
           999.99 AS amount, 'EUR' AS currency, 'debit' AS txn_type,
           'MediaMarkt' AS merchant_name, false AS is_international, 0.00 AS fee_amount,
           '{"category":"electronics","mcc":"5732","risk_score":0.06}' AS txn_metadata
    UNION ALL
    SELECT 'TXN-012' AS txn_id, 'ACC-J' AS account_id, 1700500000012 AS txn_ts,
           TIMESTAMP '2024-01-18 14:00:00' AS txn_datetime, DATE '2024-01-18' AS txn_date,
           350.00 AS amount, 'GBP' AS currency, 'debit' AS txn_type,
           'British Airways' AS merchant_name, true AS is_international, 7.50 AS fee_amount,
           '{"category":"travel","mcc":"3000","risk_score":0.09}' AS txn_metadata
);

-- ============================================================================
-- INSERT: Add more records to create file groups for clustering
-- ============================================================================
INSERT INTO v9_txns_cow_complex_nometa VALUES
    ('TXN-013', 'ACC-K', 1700600000013, TIMESTAMP '2024-01-19 08:00:00', DATE '2024-01-19',
     750.00, 'USD', 'debit', 'Best Buy', false, 0.00,
     '{"category":"electronics","mcc":"5732","risk_score":0.05}', 'us'),
    ('TXN-014', 'ACC-L', 1700600000014, TIMESTAMP '2024-01-19 09:30:00', DATE '2024-01-19',
     125.50, 'USD', 'debit', 'Uber', false, 0.00,
     '{"category":"transport","mcc":"4121","risk_score":0.03}', 'us');

INSERT INTO v9_txns_cow_complex_nometa VALUES
    ('TXN-015', 'ACC-M', 1700700000015, TIMESTAMP '2024-01-20 10:00:00', DATE '2024-01-20',
     4500.00, 'USD', 'debit', 'Japan Airlines', true, 50.00,
     '{"category":"travel","mcc":"3000","risk_score":0.11}', 'apac'),
    ('TXN-016', 'ACC-N', 1700700000016, TIMESTAMP '2024-01-20 12:00:00', DATE '2024-01-20',
     88.00, 'USD', 'debit', 'GrabFood', false, 0.00,
     '{"category":"food","mcc":"5812","risk_score":0.02}', 'apac');

-- ============================================================================
-- CLUSTERING: Manually trigger clustering to reorganize files
-- ============================================================================
CALL run_clustering(table => 'v9_txns_cow_complex_nometa');

-- ============================================================================
-- INSERT: Add records after clustering
-- ============================================================================
INSERT INTO v9_txns_cow_complex_nometa VALUES
    ('TXN-017', 'ACC-O', 1700800000017, TIMESTAMP '2024-01-21 15:00:00', DATE '2024-01-21',
     199.99, 'USD', 'debit', 'Apple Store', false, 0.00,
     '{"category":"electronics","mcc":"5732","risk_score":0.04}', 'us'),
    ('TXN-018', 'ACC-P', 1700800000018, TIMESTAMP '2024-01-21 16:30:00', DATE '2024-01-21',
     55.00, 'EUR', 'debit', 'Spotify', false, 0.00,
     '{"category":"subscription","mcc":"4899","risk_score":0.01}', 'eu');
