.mode csv
.output results/output/query_12.csv

SELECT DISTINCT p.PointId, p.Geom, i.InstantId, i.Instant,
    v1.Licence AS Licence1, v2.Licence AS Licence2
FROM Trips t1, Vehicles v1, Trips t2, Vehicles v2, Points1 p, Instants1 i
WHERE
    t1.VehicleId = v1.VehicleId
    AND t2.VehicleId = v2.VehicleId
    AND t1.VehicleId < t2.VehicleId
    AND t1.Trip @> stbox(p.Geom::WKB_BLOB, i.Instant)
    AND t2.Trip @> stbox(p.Geom::WKB_BLOB, i.Instant)
    AND valueAtTimestamp(t1.Trip, i.Instant) = p.Geom
    AND valueAtTimestamp(t2.Trip, i.Instant) = p.Geom
ORDER BY p.PointId, i.InstantId, v1.Licence, v2.Licence;