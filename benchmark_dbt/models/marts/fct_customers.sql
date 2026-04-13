-- Customer count and churn metrics
SELECT
    COUNT(*) AS total_customers,
    SUM(CASE WHEN is_churned THEN 1 ELSE 0 END) AS churned,
    ROUND(SUM(CASE WHEN is_churned THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS churn_pct
FROM public.customers
