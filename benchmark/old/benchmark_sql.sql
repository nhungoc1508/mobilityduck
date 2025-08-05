-- Large-Scale Temporal Type Benchmark: TINT vs TINT2
-- Focus: 100,000 rows, full-table aggregate queries

PRAGMA enable_profiling;
PRAGMA profiling_output_mode='json';
PRAGMA profiling_output='benchmark_results.json';

-- Create large benchmark tables for instants
DROP TABLE IF EXISTS big_tint;
DROP TABLE IF EXISTS big_tint2;

CREATE TABLE big_tint (id INTEGER, tint TINT);
CREATE TABLE big_tint2 (id INTEGER, tint2 TINT2);

-- Generate 100,000 rows of test data (instants)
INSERT INTO big_tint 
SELECT 
    range as id,
    TINT(
        (range % 1000) + 1,  -- value: 1-1000
        '2023-01-01 12:00:00'::TIMESTAMP + (range % 86400) * INTERVAL '1' SECOND  -- timestamp: spread across a day
    ) as tint
FROM range(100000);

INSERT INTO big_tint2 
SELECT 
    range as id,
    TINT2(
        (range % 1000) + 1,  -- value: 1-1000
        '2023-01-01 12:00:00'::TIMESTAMP + (range % 86400) * INTERVAL '1' SECOND  -- timestamp: spread across a day
    ) as tint2
FROM range(100000);

-- Create large benchmark tables for sequences
DROP TABLE IF EXISTS big_tint_seq;
DROP TABLE IF EXISTS big_tint2_seq;

CREATE TABLE big_tint_seq AS
SELECT
    range as id,
    '[1@2023-01-01, 2@2023-01-02, 3@2023-01-03]'::TINT as tint_sequence
FROM range(100000);

CREATE TABLE big_tint2_seq AS
SELECT 
    range as id,
    '[1@2023-01-01, 2@2023-01-02, 3@2023-01-03]'::TINT2 as tint2_sequence
FROM range(100000);

-- Large-scale benchmarks
-- Each query processes all 100,000 rows

-- 1. getValue aggregate
SELECT 'getValue TINT' as operation, COUNT(*) as count, SUM(getValue(tint)) as sum_value FROM big_tint;
SELECT 'getValue TINT2' as operation, COUNT(*) as count, SUM(getValue(tint2)) as sum_value FROM big_tint2;

-- 2. tempSubtype distinct count
SELECT 'tempSubtype TINT' as operation, COUNT(DISTINCT tempSubtype(tint)) as distinct_subtypes FROM big_tint;
SELECT 'tempSubtype TINT2' as operation, COUNT(DISTINCT tempSubtype(tint2)) as distinct_subtypes FROM big_tint2;

-- 3. getTimestamp count
SELECT 'getTimestamp TINT' as operation, COUNT(getTimestamp(tint)) as timestamp_count FROM big_tint;
SELECT 'getTimestamp TINT2' as operation, COUNT(getTimestamp(tint2)) as timestamp_count FROM big_tint2;

-- 4. sequences count (on sequence tables only)
SELECT 'sequences TINT' as operation, COUNT(sequences(tint_sequence)) as sequences_count FROM big_tint_seq;
SELECT 'sequences TINT2' as operation, COUNT(sequences(tint2_sequence)) as sequences_count FROM big_tint2_seq;

-- 5. Complex: Group by value ranges
SELECT 'GroupBy TINT' as operation, 
       (getValue(tint) / 100) as value_group, 
       COUNT(*) as count 
FROM big_tint 
GROUP BY (getValue(tint) / 100) 
ORDER BY value_group;

SELECT 'GroupBy TINT2' as operation, 
       (getValue(tint2) / 100) as value_group, 
       COUNT(*) as count 
FROM big_tint2 
GROUP BY (getValue(tint2) / 100) 
ORDER BY value_group;

-- 6. Complex: Window function
SELECT 'Window TINT' as operation, 
       id, 
       getValue(tint) as value,
       ROW_NUMBER() OVER (ORDER BY getValue(tint)) as row_num
FROM big_tint 
WHERE id < 1000;  -- Limit for readability

SELECT 'Window TINT2' as operation, 
       id, 
       getValue(tint2) as value,
       ROW_NUMBER() OVER (ORDER BY getValue(tint2)) as row_num
FROM big_tint2 
WHERE id < 1000;  -- Limit for readability

-- Clean up
DROP TABLE big_tint;
DROP TABLE big_tint2;
DROP TABLE big_tint_seq;
DROP TABLE big_tint2_seq; 