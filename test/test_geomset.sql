SELECT geomset '{"Point(1 1)", "Point(2 1)"}';

SELECT geomset '{"Point(1.1112 1.223)", "Point(2 1)"}';

SELECT geomset '{"Polygon((1 1,1 2,2 2,2 1,1 1))"}';

SELECT geomset 'SRID=5676;{"Linestring(1 1,2 2)","Polygon((1 1,1 2,2 2,2 1,1 1))"}';

SELECT asText(geomset '{"Point(1 1)", "Point(2 1)"}');

SELECT asTexT(geomset '{"Polygon((1 1,1 2,2 2,2 1,1 1))"}');

SELECT memSize(geomset '{"Point(1 1)", "Point(2 1)"}');

SELECT SRID(geomset '{Point(1 1), Point(2 2)}');

SELECT SRID(geomset 'SRID=5676;{"Linestring(1 1,2 2)","Polygon((1 1,1 2,2 2,2 1,1 1))"}');

SELECT set_srid(geomset '{POINT(1 1), POINT(2 2)}', 4326);

-- SELECT asText(set_SRID(geomset ('{Point(1 1), Point(2 2)}'), 5676));

SELECT transform(geomset 'SRID=4326;{"Point(2.340088 49.400250)", "Point(6.575317 51.553167)"}', 3812);

-- SELECT asText(transform(geomset ('SRID=5676;{"Linestring(1 1,2 2)","Polygon((1 1,1 2,2 2,2 1,1 1))"}'), 4326));

SELECT startValue(geomset '{"Point(1 1)", "Point(2 1)"}')::GEOMETRY;

SELECT endValue(geomset('{"Point(1 1)", "Point(2 1)"}'));

SELECT startValue(geomset'{"Point(1 1)", "Point(2 1)"}');