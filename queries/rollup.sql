SELECT TIME_FLOOR(__time, 'PT1M') AS minute, store_id, currency,
       SUM(events) AS event_count, SUM(amount_cents_sum) AS net_amount_cents
FROM orders_minute
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY 1, 2, 3
ORDER BY 1 DESC
LIMIT 100
