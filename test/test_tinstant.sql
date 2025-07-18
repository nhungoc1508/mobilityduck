-- Test TInstant type and functions
-- This test verifies that the TInstant integration with DuckDB works correctly

-- Test TINSTANT_MAKE function
SELECT TINSTANT_MAKE(42, 1, '2023-01-01 12:00:00'::TIMESTAMPTZ) as tinst;

-- Test TINSTANT_VALUE function
SELECT TINSTANT_VALUE(TINSTANT_MAKE(42, 1, '2023-01-01 12:00:00'::TIMESTAMPTZ)) as value;

-- Test TINSTANT_TO_STRING function
SELECT TINSTANT_TO_STRING(TINSTANT_MAKE(42, 1, '2023-01-01 12:00:00'::TIMESTAMPTZ)) as str;

-- Test with different values
SELECT TINSTANT_MAKE(100, 2, '2023-06-15 15:30:00'::TIMESTAMPTZ) as tinst2;
SELECT TINSTANT_VALUE(TINSTANT_MAKE(100, 2, '2023-06-15 15:30:00'::TIMESTAMPTZ)) as value2;
SELECT TINSTANT_TO_STRING(TINSTANT_MAKE(100, 2, '2023-06-15 15:30:00'::TIMESTAMPTZ)) as str2; 

-- Test creating table with TInstant type
CREATE TABLE tinst_table (id INTEGER PRIMARY KEY, tinst TInstant);
-- Insert data into the TInstant table
INSERT INTO tinst_table VALUES
(1, TINSTANT_MAKE(42, 1, '2023-01-01 12:00:00'::TIMESTAMPTZ)),
(2, TINSTANT_MAKE(100, 2, '2023-06-15 15:30:00'::TIMESTAMPTZ));
-- Test querying from tinst_table
SELECT * FROM tinst_table;
-- Show strings from tinst_table
SELECT TINSTANT_TO_STRING(tinst) as str FROM tinst_table;

-- UNIMPLEMENTED, FROM POSTGRES: Constructors for temporal types having constant values
SELECT tbool(true, timestamptz '2001-01-01');
SELECT tint(1, timestamptz '2001-01-01');
SELECT tfloat(1.5, tstzset '{2001-01-01, 2001-01-02}');
SELECT ttext('AAA', tstzset '{2001-01-01, 2001-01-02}');
SELECT tfloat(1.5, tstzspan '[2001-01-01, 2001-01-02]');
SELECT tfloat(1.5, tstzspan '[2001-01-01, 2001-01-02]', 'step');
SELECT tgeompoint('Point(0 0)', tstzspan '[2001-01-01, 2001-01-02]');
SELECT tgeography('SRID=7844;Point(0 0)', tstzspanset '{[2001-01-01, 2001-01-02],
  [2001-01-03, 2001-01-04]}', 'step');