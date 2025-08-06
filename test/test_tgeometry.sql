SELECT tgeometry('[Point(0 0)@2017-01-01 08:00:00, Linestring(0 0,0 1)@2017-01-02 08:05:00]');

SELECT tgeometry '[Point(0 0)@2017-01-01 08:00:00, Linestring(0 0,0 1)@2017-01-02 08:05:00]';

SELECT astext(tgeometry 'Point(1 1)@2023-01-01 10:00:00+00');
--***POINT(1 1)@2023-01-01 10:00:00+00  

SELECT asEWKT(tgeometry('Point(1 1)', timestamptz '2012-01-01 08:00:00'));
--***POINT(1 1)@2012-01-01 08:00:00+00 

SELECT asEWKT(tgeometry('Point(1 1)', tstzspan '[2001-01-01, 2001-01-02]'));
--***POINT(1 1)@2012-01-01 08:00:00+00 

SELECT asewkt(tgeometry 'Point(1 1)@2023-01-01 10:00:00+00');
--***POINT(1 1)@2023-01-01 10:00:00+00  

SELECT timeSpan(tgeometry('Point(1 1)@2023-01-01 10:00:00+00'));
SELECT timeSpan(tgeometry 'Point(1 1)@2023-01-01 10:00:00+00');
--***[2023-01-01 10:00:00+00, 2023-01-01 10:00:00+00]  

SELECT tgeometryInst(tgeometry('Point(1 1)@2023-01-01 10:00:00+00'));


SELECT asewkt(tgeometry('[Point(0 0)@2017-01-01 08:00:00, Linestring(0 0,0 1)@2017-01-02 08:05:00]'));
--***[POINT(0 0)@2017-01-01 08:00:00+00, LINESTRING(0 0,0 1)@2017-01-02 08:05:00+00]  


SELECT asText(tgeometry ('Point(1 1)@2012-01-01 08:00:00'));
--***POINT(1 1)@2012-01-01 08:00:00+00   

SELECT asText(tgeometry (' { Point(1 1)@2001-01-01 08:00:00 , Point(2 2)@2001-01-01 08:05:00 , Point(3 3)@2001-01-01 08:06:00 } '));

SELECT asText(tgeometry (' [ Point(1 1)@2001-01-01 08:00:00 , Point(2 2)@2001-01-01 08:05:00 , Point(3 3)@2001-01-01 08:06:00 ] '));


SELECT tgeometry('[Point(1 1)@2001-01-01 08:00:00,Point empty@2001-01-01 08:05:00,Point(3 3)@2001-01-01 08:06:00]');
--Only non-empty geometries accepted


select tgeometrySeq('{Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03}', 'discrete');


SELECT asEWKT(tgeometrySeq(tgeometry '[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03]', 'step'));
SELECT asEWKT(tgeometrySeq(tgeometry '[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03]'));
--***[POINT(1 1)@2000-01-01 00:00:00+00, POINT(2 2)@2000-01-02 00:00:00+00, POINT(1 1)@2000-01-03 00:00:00+00]     

SELECT tgeometrySeq(ARRAY[tgeometry 'Point(1 1)@2001-01-01', 'Point(2 2 2)@2001-01-02'], 'discrete');
--The arguments must be of the same dimensionality

SELECT asEWKT(tgeometrySeq(ARRAY[tgeometry 'Point(1 1)@2001-01-01', 'Point(2 2 )@2001-01-02'], 'discrete'));
-- --***POINT(1 1)@2001-01-01 00:00:00+00, POINT(2 2)@2001-01-02 00:00:00+00}




SELECT asEWKT(setInterp(tgeometry 'Point(1 1)@2000-01-01', 'discrete'));
--***{POINT(1 1)@2000-01-01 00:00:00+00}    

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

-- select getValue(tgeometry 'Point(1 1)@2000-01-01');
-- --*** POINT(1 1)   

-- select valueN(tgeometry '{Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03}', 1);
-- --*** POINT(1 1)   

-- select endValue(tgeometry '{[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03],[Point(3 3)@2000-01-04, Point(3 3)@2000-01-05]}')
-- --***POINT(3 3) 

-- SELECT asEWKT(startInstant(tgeometry 'Point(1 1)@2000-01-01'));
-- SELECT asEWKT(startInstant(tgeometry '{Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03}'));
-- SELECT asEWKT(startInstant(tgeometry '[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03]'));
-- SELECT asEWKT(startInstant(tgeometry '{[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03],[Point(3 3)@2000-01-04, Point(3 3)@2000-01-05]}'));
-- --***POINT(1 1)@2000-01-01 00:00:00+00  


-- SELECT asEWKT(instantN(tgeometry 'Point(1 1)@2000-01-01', 1));
-- SELECT asEWKT(instantN(tgeometry '{Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03}', 1));
-- SELECT asEWKT(instantN(tgeometry '[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03]', 1));
-- SELECT asEWKT(instantN(tgeometry '{[Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03],[Point(3 3)@2000-01-04, Point(3 3)@2000-01-05]}', 1));
-- --***POINT(1 1)@2000-01-01 00:00:00+00 


-- select st_astext(startValue(tgeometry 'Point(1 1)@2000-01-01'));



SELECT getTimestamp(tgeometry 'Point(1 1)@2023-01-01 10:00:00+00');
--*** 2023-01-01 10:00:00+00 


