-- HiveQL to find top 5 Most Visited Destinations

SELECT dest,COUNT(dest) as x FROM aviation
GROUP BY dest
ORDER BY x DESC
LIMIT 5;