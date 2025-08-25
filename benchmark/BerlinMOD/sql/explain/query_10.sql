.output results/output/explain/query_10.txt

EXPLAIN ANALYZE
WITH Values AS (
    SELECT DISTINCT l1.Licence AS QueryLicence, l2.Licence AS OtherLicence,
        atTime(t1.Trip, getTime(atValues(tdwithin(t1.Trip, t2.Trip, 3.0), TRUE))) AS Pos
    FROM Trips t1, Licences1 l1, Trips t2, Licences2 l2
    WHERE t1.VehicleId = l1.VehicleId AND t2.VehicleId = l2.VehicleId AND
        t1.VehicleId < t2.VehicleId AND
        expandSpace(t1.Trip::STBOX, 3) && expandSpace(t2.Trip::STBOX, 3) AND
        eDwithin(t1.Trip, t2.Trip, 3.0) )
SELECT QueryLicence, OtherLicence, array_agg(Pos ORDER BY startTimestamp(Pos)) AS Pos
FROM Values
GROUP BY QueryLicence, OtherLicence
ORDER BY QueryLicence, OtherLicence;