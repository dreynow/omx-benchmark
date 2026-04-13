-- Quarterly revenue trend
SELECT
    DATE_TRUNC('quarter', invoice_date) AS quarter,
    SUM(total_amount) AS revenue
FROM public.invoices
WHERE NOT is_cancellation
GROUP BY 1
ORDER BY 1
