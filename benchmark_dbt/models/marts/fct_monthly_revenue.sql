-- Monthly revenue trend
SELECT
    DATE_TRUNC('month', invoice_date) AS month,
    SUM(total_amount) AS revenue
FROM public.invoices
WHERE NOT is_cancellation
GROUP BY 1
ORDER BY 1
