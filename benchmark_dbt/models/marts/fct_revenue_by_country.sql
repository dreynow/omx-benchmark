-- Revenue breakdown by country
SELECT
    country,
    SUM(total_amount) AS revenue
FROM public.invoices
WHERE NOT is_cancellation
GROUP BY country
ORDER BY revenue DESC
