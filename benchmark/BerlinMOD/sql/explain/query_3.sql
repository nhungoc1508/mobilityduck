.output results/output/explain/query_3.txt

EXPLAIN ANALYZE
SELECT DISTINCT l.Licence, i.InstantId, i.Instant AS Instant,
    valueAtTimestamp(t.Trip, i.Instant)::GEOMETRY AS Pos
FROM Trips t, Licences1 l, Instants1 i
WHERE t.VehicleId = l.VehicleId AND valueAtTimestamp(t.Trip, i.Instant) IS NOT NULL
ORDER BY l.Licence, i.InstantId;