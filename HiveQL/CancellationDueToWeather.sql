-- HiveQL to find the month with most number of cancellations due to bad weather

SELECT month,COUNT(canceled) as t FROM aviation
WHERE canceled = 1 AND canel_code = ‘B’
GROUP BY month
ORDER BY t DESC
LIMIT 1;