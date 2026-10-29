SELECT TIME_FLOOR(__time, 'PT1M') AS event_minute, store_id, currency,
       COUNT(*) AS event_count, SUM(amount_cents) AS net_amount_cents
FROM orders_raw
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY 1, 2, 3
ORDER BY 1 DESC
LIMIT 100
