-- Total revenue excluding cancellations
SELECT
    SUM(total_amount) AS total_revenue
FROM public.invoices
WHERE NOT is_cancellation
