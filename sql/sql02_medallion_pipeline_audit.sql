-- 1. Total de eventos crudos en Bronze
SELECT count(*) AS total_bronze 
FROM delta.`s3a://AKIAWTL3NUA52H2ZXUPY:omRKcv0gtMSaS2H5Hf5cfloHOg6%2FgHTOEU47MbyR@ecommerce-streaming-jordi-2026/delta/bronze_ecommerce_events`;

-- 2. Total de transacciones limpias y curadas en Silver
SELECT count(*) AS total_silver 
FROM delta.`s3a://AKIAWTL3NUA52H2ZXUPY:omRKcv0gtMSaS2H5Hf5cfloHOg6%2FgHTOEU47MbyR@ecommerce-streaming-jordi-2026/delta/silver_ecommerce_orders`;

-- 3. Métricas consolidadas en Gold (Ingresos globales y clientes totales)
SELECT 
    sum(total_revenue) AS ingresos_totales_usd,
    sum(unique_customers) AS clientes_totales,
    sum(total_orders) AS total_pedidos
FROM delta.`s3a://AKIAWTL3NUA52H2ZXUPY:omRKcv0gtMSaS2H5Hf5cfloHOg6%2FgHTOEU47MbyR@ecommerce-streaming-jordi-2026/delta/gold_sales_by_country`;
