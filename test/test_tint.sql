-- Test TInt type and functions
-- This test verifies that the TInt integration with DuckDB works correctly

-- Test TINT function
SELECT TINT(42, '2023-01-01 12:00:00'::TIMESTAMPTZ) as tint;

-- Test TINT_VALUE function
SELECT TINT_VALUE(TINT_MAKE(42, '2023-01-01 12:00:00'::TIMESTAMPTZ)) as value;

-- Test TINT_TO_STRING function
SELECT TINT_TO_STRING(TINT_MAKE(42, '2023-01-01 12:00:00'::TIMESTAMPTZ)) as str;