-- Top products by revenue
SELECT
    p.description,
    p.stock_code,
    SUM(ii.quantity) AS units_sold,
    SUM(ii.line_total) AS revenue
FROM public.invoice_items ii
JOIN public.products p ON p.id = ii.product_id
JOIN public.invoices i ON i.id = ii.invoice_id
WHERE NOT i.is_cancellation AND ii.quantity > 0
GROUP BY p.description, p.stock_code
ORDER BY revenue DESC
