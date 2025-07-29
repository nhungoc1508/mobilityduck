-- Test Intset 

SELECT INTSET('{-1,-3,3}');

SELECT FLOATSET('{-1.2,-3.1,3}');

SELECT tstzset('{2001-01-01 08:00:00, 2001-01-03 09:30:00}'); 

SELECT textset('{"highway", "car"}');

SELECT dateset('{2001-02-01, 2018-02-01}');


-- Test table creating
CREATE TABLE intset_table (id INTEGER PRIMARY KEY, val INTSET);
-- Insert data into the  table
INSERT INTO intset_table VALUES
(1, INTSET('{-1,-3,3}')),
(2, INTSET('{1,2,3}'));
-- Test querying from intset_table
SELECT * FROM intset_table;

-- Test asText function: 
SELECT asText(INTSET('{3,1,-2}')); -- → "{-2,1,3}"

SELECT asText(INTSET('{-1,-3,3}'));

SELECT asText(FLOATSET('{-1.2,-3.1,3}'));

SELECT asText(tstzset('{2001-01-01 08:00:00, 2001-01-03 09:30:00}')); 

SELECT asText(textset('{"highway", "car", "bike"}'));

SELECT asText(dateset('{2001-02-01, 2018-02-01}'));


-- Test set Constructor from List:
SELECT set([1, 2, 3]);
SELECT set([1.2,1.5]);
SELECT set([DATE'2001-01-02', DATE '2025-01-01']);
SELECT set([TIMESTAMPTZ'2001-01-01 08:00:00', TIMESTAMPTZ '2025-01-01 08:00:00']); 
SELECT set(['car', 'bus']); 

-- Test Conversion function:

SELECT Set(5);         -- returns: "{5}"
SELECT Set(-42);       -- returns: "{-42}"
SELECT Set(1.5);
SELECT Set(TIMESTAMPTZ'2001-01-01 08:00:00');
SELECT Set(DATE'2001-01-01') 
-- TEXT need testing later 


-- Test MemSize:
SELECT memSize(INTSET('{1,23}')); 
SELECT memSize(INTSET('{-1,-3,3}'));

SELECT memSize(FLOATSET('{-1.2,-3.1,3}'));

SELECT memSize(tstzset('{2001-01-01 08:00:00, 2001-01-03 09:30:00}')); 

SELECT memSize(textset('{"highway"}'));

SELECT memSize(dateset('{2001-02-01, 2001-02-18}'));

-- Test numValues: 
SELECT numValues(intset('{1,3,5,7}'));
SELECT numValues(FLOATSET('{-1.2,-3.1,3}'));

SELECT numValues(tstzset('{2001-01-01 08:00:00, 2001-01-03 09:30:00}')); 

SELECT numValues(textset('{"highway", "car", "bike"}'));

SELECT numValues(dateset('{2001-02-01, 2022-02-18}'));


-- Test startValue: 
SELECT startValue(INTSET('{1,3,5,7}'));  -- → 1
SELECT startValue(INTSET('{42, 0, 7}'));  -- → 0
SELECT startValue(intset('{1,3,5,7}'));
SELECT startValue(FLOATSET('{-1.2,-3.1,3}'));

SELECT startValue(textset('{"highway", "car", "bike"}')); 

SELECT startValue(dateset('{2001-02-01, 2022-02-18}')); 

-- Test endValue
SELECT endValue(INTSET('{1,3,5,7}'));  -- → 7
SELECT endValue(INTSET('{42, 0, 7}'));  -- → 42

--Test ValueN
SELECT valueN(INTSET('{-2, 2, 7}'),1); -- -> -2
SELECT valueN(INTSET('{-2, 2, 7}'),4); -- -> NULL

SELECT valueN(textset('{"highway", "car", "bike"}'),2);

--Test getValues 
SELECT getValues(intset('{1,3,5,7}')); --> now return as varchar, later will consider return as list 
--Test shift
-- SELECT shift(INTSET('{-2, 2, 7}'),3);
--Test unnest 
-- Note that unnest is a built-in function in duckdb so we have to name differently 
SELECT * FROM setUnnest(INTSET('{1, 1, 5, -7, -7}'));
SELECT * FROM setUnnest(tstzset('{2001-01-01 08:00:00, 2001-01-03 09:30:00}'));
SELECT * FROM setUnnest(dateset('{2001-02-01, 2022-02-18}'));
SELECT * FROM setUnnest(textset('{"highway", "car", "bike"}'));

-- Missing these function
-- CREATE FUNCTION floatset(intset)
--   RETURNS floatset
--   AS 'MODULE_PATHNAME', 'Intset_to_floatset'
--   LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
-- CREATE FUNCTION intset(floatset)
--   RETURNS intset
--   AS 'MODULE_PATHNAME', 'Floatset_to_intset'
--   LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE CAST (intset AS floatset) WITH FUNCTION floatset(intset);
-- CREATE CAST (floatset AS intset) WITH FUNCTION intset(floatset);

-- CREATE FUNCTION tstzset(dateset)
--   RETURNS tstzset
--   AS 'MODULE_PATHNAME', 'Dateset_to_tstzset'
--   LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
-- CREATE FUNCTION dateset(tstzset)
--   RETURNS dateset
--   AS 'MODULE_PATHNAME', 'Tstzset_to_dateset'
--   LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
