-- HiveQL to find top 10 routes that have seen maximum diversion

SELECT origin,dest,COUNT(diversion) as t FROM aviation
WHERE diversion = 1
GROUP BY origin,dest
ORDER BY t DESC
LIMIT 10;