-- Consulta analítica: Top 10 productos con mayor recaudación
SELECT 
    product_name AS Producto,
    total_revenue AS Ingresos_Totales_USD,
    units_sold AS Unidades_Vendidas,
    total_orders AS Total_Ordenes,
    avg_unit_price AS Precio_Promedio_USD
FROM delta.`s3a://AKIAWTL3NUA52H2ZXUPY:omRKcv0gtMSaS2H5Hf5cfloHOg6%2FgHTOEU47MbyR@ecommerce-streaming-jordi-2026/delta/gold_top_products`
ORDER BY total_revenue DESC
LIMIT 10;