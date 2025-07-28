SELECT geomset('{"Point(1 1)", "Point(2 1)"}');

SELECT geomset('{"Point(1.1112 1.223)", "Point(2 1)"}');

SELECT geomset ('{"Polygon((1 1,1 2,2 2,2 1,1 1))"}');

SELECT asText(geomset('{"Point(1 1)", "Point(2 1)"}'));

SELECT asTexT(geomset ('{"Polygon((1 1,1 2,2 2,2 1,1 1))"}'));

SELECT memSize(geomset('{"Point(1 1)", "Point(2 1)"}'));

SELECT SRID(geomset ('{Point(1 1), Point(2 2)}'));

SELECT SRID(geomset ('SRID=5676;{"Linestring(1 1,2 2)","Polygon((1 1,1 2,2 2,2 1,1 1))"}'));

SELECT asText(setSRID(geomset ('{Point(1 1), Point(2 2)}'), 5676));