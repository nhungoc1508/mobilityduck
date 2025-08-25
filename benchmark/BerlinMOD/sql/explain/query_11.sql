.output results/output/explain/query_11.txt

EXPLAIN ANALYZE
SELECT p.PointId, p.Geom, i.InstantId, i.Instant, v.Licence
FROM Trips t, Vehicles v, Points1 p, Instants1 i
WHERE
    t.VehicleId = v.VehicleId
    AND t.Trip @> stbox(p.Geom::WKB_BLOB, i.Instant)
    AND valueAtTimestamp(t.Trip, i.Instant) = p.Geom
ORDER BY p.PointId, i.InstantId, v.Licence;