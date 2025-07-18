-- Test TInt type and functions
-- This test verifies that the TInt integration with DuckDB works correctly

-- Test TINT function
SELECT TINT(42, '2023-01-01 12:00:00'::TIMESTAMPTZ) as tint;

-- Test getValue function
SELECT getValue(TINT(42, '2023-01-01 12:00:00'::TIMESTAMPTZ)) as value;

-- Test TINT_TO_STRING function
SELECT TINT_TO_STRING(TINT(42, '2023-01-01 12:00:00'::TIMESTAMPTZ)) as str;

-- Test creating table with TInt type
CREATE TABLE tint_table (id INTEGER PRIMARY KEY, val TINT);
-- Insert data into the TInt table
INSERT INTO tint_table VALUES
(1, TINT(42, '2023-01-01 12:00:00'::TIMESTAMPTZ)),
(2, TINT(100, '2023-06-15 15:30:00'::TIMESTAMPTZ));
-- Test querying from tint_table
SELECT * FROM tint_table;
-- Show strings from tint_table
SELECT TINT_TO_STRING(val) as str FROM tint_table;