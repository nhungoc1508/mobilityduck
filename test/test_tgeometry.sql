SELECT tgeometry('[Point(0 0)@2017-01-01 08:00:00, Linestring(0 0,0 1)@2017-01-02 08:05:00]');
--[010100000000000000000000000000000000000000@2017-01-01 08:00:00+00, 010200000002000000000000000000000000000000000000000000000000000000000000000000F03F@2017-01-02 08:05:00+00]

SELECT tgeometry '[Point(0 0)@2017-01-01 08:00:00, Linestring(0 0,0 1)@2017-01-02 08:05:00]';

SELECT astext(tgeometry 'Point(1 1)@2023-01-01 10:00:00+00');
--***POINT(1 1)@2023-01-01 10:00:00+00  

SELECT asEWKT(tgeometry('Point(1 1)', timestamptz '2012-01-01 08:00:00'));
--***POINT(1 1)@2012-01-01 08:00:00+00 

SELECT asEWKT(tgeometry('Point(1 1)', tstzspan '[2001-01-01, 2001-01-02]'));
--***[POINT(1 1)@2001-01-01 00:00:00+00, POINT(1 1)@2001-01-02 00:00:00+00]

SELECT asEWKT(tgeometry('Point(1 1)', tstzspan '[2001-01-01, 2001-01-02]', 'step'));
--***[POINT(1 1)@2001-01-01 00:00:00+00, POINT(1 1)@2001-01-02 00:00:00+00]

SELECT asewkt(tgeometry 'Point(1 1)@2023-01-01 10:00:00+00');
--***POINT(1 1)@2023-01-01 10:00:00+00  

SELECT asEWKT(tgeometrySeq(ARRAY[tgeometry 'Point(1 1)@2001-01-01', 'Point(2 2 )@2001-01-02'], 'step', 'true','true'));
SELECT asEWKT(tgeometrySeq(ARRAY[tgeometry 'Point(1 1)@2001-01-01', 'Point(2 2 )@2001-01-02'], 'step', 'true'));
SELECT asEWKT(tgeometrySeq(ARRAY[tgeometry 'Point(1 1)@2001-01-01', 'Point(2 2 )@2001-01-02'], 'step'));
SELECT asEWKT(tgeometrySeq(ARRAY[tgeometry 'Point(1 1)@2001-01-01', 'Point(2 2 )@2001-01-02']));
--***[POINT(1 1)@2001-01-01 00:00:00+00, POINT(2 2)@2001-01-02 00:00:00+00]     


SELECT asEWKT(tgeometrySeq(ARRAY[tgeometry 'Point(1 1)@2001-01-01', 'Point(2 2 )@2001-01-02'], 'discrete'));
-- --***POINT(1 1)@2001-01-01 00:00:00+00, POINT(2 2)@2001-01-02 00:00:00+00}


SELECT asewkt(tgeometry('[Point(0 0)@2017-01-01 08:00:00, Linestring(0 0,0 1)@2017-01-02 08:05:00]'));
--***[POINT(0 0)@2017-01-01 08:00:00+00, LINESTRING(0 0,0 1)@2017-01-02 08:05:00+00]  

SELECT asText(tgeometry ('Point(1 1)@2012-01-01 08:00:00'));
--***POINT(1 1)@2012-01-01 08:00:00+00   

SELECT asText(tgeometry (' { Point(1 1)@2001-01-01 08:00:00 , Point(2 2)@2001-01-01 08:05:00 , Point(3 3)@2001-01-01 08:06:00 } '));

SELECT asText(tgeometry (' [ Point(1 1)@2001-01-01 08:00:00 , Point(2 2)@2001-01-01 08:05:00 , Point(3 3)@2001-01-01 08:06:00 ] '));

SELECT timeSpan(tgeometry('Point(1 1)@2023-01-01 10:00:00+00'));
SELECT timeSpan(tgeometry 'Point(1 1)@2023-01-01 10:00:00+00');
--***[2023-01-01 10:00:00+00, 2023-01-01 10:00:00+00]  

SELECT tgeometryInst(tgeometry('Point(1 1)@2023-01-01 10:00:00+00'));
--***0101000000000000000000F03F000000000000F03F@2023-01-01 10:00:00+00

SELECT asEWKT(setInterp(tgeometry 'Point(1 1)@2000-01-01', 'discrete'));
--***{POINT(1 1)@2000-01-01 00:00:00+00}  

SELECT asEWKT(setInterp(tgeometry 'Point(1 1)@2000-01-01', 'step'));
--***[POINT(1 1)@2000-01-01 00:00:00+00]

SELECT asEWKT(setInterp(tgeometry '{Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03}', 'discrete'));
--***{POINT(1 1)@2000-01-01 00:00:00+00, POINT(2 2)@2000-01-02 00:00:00+00, POINT(1 1)@2000-01-03 00:00:00+00}  

SELECT asText(merge(tgeometry 'Point(1 1)@2000-01-01', tgeometry 'Point(1 1)@2000-01-02'));
--***{POINT(1 1)@2000-01-01 00:00:00+00, POINT(1 1)@2000-01-02 00:00:00+00}


SELECT tempSubtype(tgeometry 'Point(1 1)@2000-01-01');
--**Instant 
SELECT tempSubtype(tgeometry '{Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03}');
--**Sequence
SELECT tempSubtype(tgeometry '[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03]');
--**Sequence
SELECT tempSubtype(tgeometry '{[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03],[Point(3 3)@2000-01-04, Point(3 3)@2000-01-05]}');
--**SequenceSet

SELECT memSize(tgeometry 'Point(1 1)@2000-01-01') > 0;
SELECT memSize(tgeometry '{Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03}') > 0;
SELECT memSize(tgeometry '[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03]') > 0;
SELECT memSize(tgeometry '{[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03],[Point(3 3)@2000-01-04, Point(3 3)@2000-01-05]}') > 0;
--***true

select getValue(tgeometry 'Point(1 1)@2000-01-01')::GEOMETRY;
--*** POINT (1 1) 

select startValue(tgeometry '{[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03],[Point(3 3)@2000-01-04, Point(3 3)@2000-01-05]}')::GEOMETRY
-- --*** POINT (1 1) 

select endValue(tgeometry '{[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03],[Point(3 3)@2000-01-04, Point(3 3)@2000-01-05]}')::GEOMETRY
--***POINT (3 3) 

SELECT asEWKT(startInstant(tgeometry 'Point(1 1)@2000-01-01'));
SELECT asEWKT(startInstant(tgeometry '{Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03}'));
SELECT asEWKT(startInstant(tgeometry '[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03]'));
SELECT asEWKT(startInstant(tgeometry '{[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03],[Point(3 3)@2000-01-04, Point(3 3)@2000-01-05]}'));
--***POINT(1 1)@2000-01-01 00:00:00+00  


SELECT asEWKT(endInstant(tgeometry 'Point(3 3)@2000-01-05'));
SELECT asEWKT(endInstant(tgeometry '{Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(3 3)@2000-01-05}'));
SELECT asEWKT(endInstant(tgeometry '[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(3 3)@2000-01-05]'));
SELECT asEWKT(endInstant(tgeometry '{[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03],[Point(3 3)@2000-01-04, Point(3 3)@2000-01-05]}'));
--***POINT(3 3)@2000-01-05 00:00:00+00

SELECT asEWKT(instantN(tgeometry 'Point(1 1)@2000-01-01', 1));
SELECT asEWKT(instantN(tgeometry '{Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03}', 1));
SELECT asEWKT(instantN(tgeometry '[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03]', 1));
SELECT asEWKT(instantN(tgeometry '{[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03],[Point(3 3)@2000-01-04, Point(3 3)@2000-01-05]}', 1));
--***POINT(1 1)@2000-01-01 00:00:00+00 



SELECT getTimestamp(tgeometry 'Point(1 1)@2023-01-01 10:00:00+00');
--*** 2023-01-01 10:00:00+00 



CREATE TABLE test_geo(geo geometry, time_span tstzspan);
CREATE TABLE test_geo(
    "geo" geometry,
    "times" timestamptz
);
INSERT INTO test_geo VALUES
    (ST_Point(1, 1), '2025-08-11 12:00:00+00'),
    (ST_Point(2.5, 3.5), '2025-08-11 15:30:00+02'),
    (ST_GeomFromText('LINESTRING(0 0, 2 2, 4 0)'), '2025-08-10 08:15:00+00'),
    (ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'), '2025-08-09 20:00:00+02');
SELECT asEWKT(tgeometry(geo, times)) FROM test_geo;
----POINT(1 1)@2025-08-11 12:00:00+00
----POINT(2 4)@2025-08-11 13:30:00+00
----LINESTRING(0 0,2 2,4 0)@2025-08-10 08:15:00+00
----POLYGON((0 0,0 5,5 5,5 0,0 0))@2025-08-09 18:00:00+00 


