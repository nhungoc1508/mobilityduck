.mode csv
.output results/output/query_14.csv

SELECT DISTINCT r.RegionId, i.InstantId, i.Instant, v.Licence
FROM Trips t, Vehicles v, Regions1 r, Instants1 i
WHERE
    t.VehicleId = v.VehicleId
    AND t.Trip && stbox(r.Geom::WKB_BLOB, i.Instant)
    AND ST_Contains(r.Geom, valueAtTimestamp(t.Trip, i.Instant)::GEOMETRY)
ORDER BY r.RegionId, i.InstantId, v.Licence;