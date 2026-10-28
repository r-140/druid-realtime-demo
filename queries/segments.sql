SELECT datasource, COUNT(*) AS segments, SUM(size) AS bytes,
       SUM(num_rows) AS rows, SUM(is_available) AS available_segments
FROM sys.segments
WHERE is_active = 1
GROUP BY 1
