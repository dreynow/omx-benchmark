-- Average order value
SELECT
    AVG(total_amount) AS avg_order_value
FROM public.invoices
