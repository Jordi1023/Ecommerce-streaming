import os
import time
import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb
from google import genai
from dotenv import load_dotenv

# Configuración de página
st.set_page_config(
    page_title="E-Commerce Lakehouse & AI Agent",
    page_icon="🛒",
    layout="wide"
)

# Cargar variables de entorno (.env)
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
S3_BUCKET = "ecommerce-streaming-jordi-2026"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Inicializar cliente oficial de Gemini
if GEMINI_API_KEY:
    client_gemini = genai.Client(api_key=GEMINI_API_KEY)
else:
    client_gemini = None

# Función para procesar consultas en 1 sola llamada con manejo conversacional y fallback
def process_agent_query(user_query, max_retries=2):
    """
    Agente Text-to-SQL de alta resiliencia:
    - Maneja preguntas conversacionales y de capacidades.
    - Genera SQL ANSI y diagnóstico para preguntas analíticas.
    - Fallback local estructurado e inteligente.
    """
    query_lower = user_query.lower()

    # Detección de preguntas sobre capacidades o saludos
    greeting_keywords = ["que haces", "que eres capaz", "quien eres", "capacidades", "ayuda", "hola"]
    if any(k in query_lower for k in greeting_keywords) and not any(k in query_lower for k in ["venta", "producto", "pais", "top", "ingreso"]):
        desc_text = """
### 🤖 Capacidades del Asistente Analítico del Lakehouse

Estoy conectado directamente a los datos consolidados en **AWS S3 / DuckDB** y puedo ayudarte con:

* **📊 Análisis de Ventas Globales:** Consultar GMV acumulado, ticket promedio y cantidad de órdenes.
* **🌍 Desempeño Geográfico:** Identificar los países con mayor y menor volumen transaccional.
* **🏆 Rendimiento de Catálogo:** Ranking de productos más vendidos por unidades e ingresos generados.
* **🔍 Generación SQL Auditable:** Traduzco tus consultas de negocio a queries SQL ANSI ejecutadas al instante en DuckDB.

*Ejemplo de preguntas:*
1. *"¿Cuáles son los 5 países con mayor facturación?"*
2. *"¿Cuál es el producto líder en unidades vendidas?"*
"""
        return "-- Consulta conversacional / Presentación de capacidades", pd.DataFrame(), desc_text

    schema_prompt = f"""
Eres un Analista de Datos Senior especializado en SQL (DuckDB) y analítica de E-Commerce.

Esquema de tablas en DuckDB:
1. `df_products` (product_name VARCHAR, total_revenue DOUBLE, units_sold BIGINT, total_orders BIGINT)
2. `df_countries` (country VARCHAR, total_revenue DOUBLE, unique_customers BIGINT, total_orders BIGINT, avg_order_value DOUBLE)

Pregunta del usuario: "{user_query}"

INSTRUCCIONES:
1. Genera una consulta SQL ANSI válida para DuckDB.
2. Escribe una respuesta ejecutiva estructurada con métricas clave, totales en USD y conclusiones comerciales.
3. Responde EXACTAMENTE en este formato separado por '---SPLIT---':

[SQL PLANO SIN BLOQUES MARKDOWN]
---SPLIT---
[ANÁLISIS EJECUTIVO EN FORMATO MARKDOWN]
"""

    VALID_MODELS = ['gemini-3.6-flash', 'gemini-3.6-flash-lite']

    if client_gemini:
        for model_name in VALID_MODELS:
            for attempt in range(max_retries):
                try:
                    response = client_gemini.models.generate_content(
                        model=model_name,
                        contents=schema_prompt,
                    )
                    full_text = response.text.strip()

                    if "---SPLIT---" in full_text:
                        parts = full_text.split("---SPLIT---")
                        sql_query = parts[0].replace("```sql", "").replace("```", "").strip()
                        interpretation = parts[1].strip()
                    else:
                        sql_query = "SELECT country, total_revenue, total_orders FROM df_countries ORDER BY total_revenue DESC LIMIT 5"
                        interpretation = full_text

                    result_df = duckdb.query(sql_query).df()
                    return sql_query, result_df, interpretation

                except Exception as e:
                    err_str = str(e)
                    if ("429" in err_str or "RESOURCE_EXHAUSTED" in err_str or "503" in err_str) and attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    break

    # =========================================================================
    # FALLBACK LOCAL AUTOMÁTICO (OFFLINE / QUOTA LIMIT)
    # =========================================================================
    if any(w in query_lower for w in ["producto", "articulo", "vendido", "item"]):
        fallback_sql = "SELECT product_name, units_sold, total_revenue FROM df_products ORDER BY total_revenue DESC LIMIT 5"
        res_df = duckdb.query(fallback_sql).df()
        top_item = res_df.iloc[0]['product_name']
        top_rev = res_df.iloc[0]['total_revenue']
        fallback_interp = f"### Resumen Ejecutivo: Productos Principales\n\nEl producto con mayor recaudación es **{top_item}**, generando un total de **${top_rev:,.2f} USD**."
    else:
        fallback_sql = "SELECT country, total_revenue, total_orders, avg_order_value FROM df_countries ORDER BY total_revenue DESC LIMIT 5"
        res_df = duckdb.query(fallback_sql).df()
        top_c = res_df.iloc[0]['country']
        top_rev = res_df.iloc[0]['total_revenue']
        fallback_interp = f"### Resumen Ejecutivo: Análisis Geográfico\n\nEl mercado líder en volumen transaccional es **{top_c}**, con una facturación acumulada de **${top_rev:,.2f} USD**."

    return fallback_sql, res_df, fallback_interp

# Función de carga de datos desde S3 (Capa Gold)
@st.cache_data(ttl=15)
def load_gold_data():
    try:
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY}';")
        con.execute(f"SET s3_secret_access_key='{AWS_SECRET_KEY}';")
        con.execute(f"SET s3_region='{AWS_REGION}';")

        top_products_path = f"s3://{S3_BUCKET}/delta/gold_top_products/*.parquet"
        country_sales_path = f"s3://{S3_BUCKET}/delta/gold_sales_by_country/*.parquet"

        raw_products = con.execute(f"SELECT * FROM read_parquet('{top_products_path}')").df()
        raw_countries = con.execute(f"SELECT * FROM read_parquet('{country_sales_path}')").df()

        # 1. Consolidar productos
        df_products = (
            raw_products.groupby("product_name", as_index=False)
            .agg({
                "total_revenue": "sum",
                "units_sold": "sum",
                "total_orders": "sum"
            })
        )

        # 2. Consolidar países y calcular ticket promedio
        df_countries = (
            raw_countries[
                raw_countries['country'].notna() & 
                (raw_countries['country'].astype(str).str.lower() != 'null')
            ]
            .groupby("country", as_index=False)
            .agg({
                "total_revenue": "sum",
                "unique_customers": "sum",
                "total_orders": "sum"
            })
        )
        df_countries["avg_order_value"] = df_countries.apply(
            lambda row: row["total_revenue"] / row["total_orders"] if row["total_orders"] > 0 else 0,
            axis=1
        )

        return df_products, df_countries
    except Exception as e:
        st.error(f"Error al leer datos desde S3: {e}")
        return pd.DataFrame(), pd.DataFrame()

# Cargar DataFrames
df_products, df_countries = load_gold_data()

# Header Principal
st.title("⚡ E-Commerce Streaming Lakehouse & AI Agent")
st.markdown("Plataforma analítica con **Databricks Medallion Architecture** y **Agente Conversacional Text-to-SQL (Gemini)**.")
st.markdown("---")

if not df_products.empty and not df_countries.empty:

    # Pestañas de Navegación
    tab_dashboard, tab_streaming, tab_agent = st.tabs([
        "📊 Dashboard Ejecutivo", 
        "⚡ Pipeline Streaming en Vivo", 
        "🤖 Agente IA (Text-to-SQL)"
    ])

    # ==========================================
    # PESTAÑA 1: DASHBOARD EJECUTIVO
    # ==========================================
    with tab_dashboard:
        total_revenue = df_countries['total_revenue'].sum()
        total_orders = df_countries['total_orders'].sum()
        total_customers = df_countries['unique_customers'].sum()
        avg_ticket = total_revenue / total_orders if total_orders > 0 else 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("💰 GMV Total (USD)", f"${total_revenue:,.2f}")
        col2.metric("📦 Pedidos Totales", f"{total_orders:,}")
        col3.metric("👥 Clientes Únicos", f"{total_customers:,}")
        col4.metric("🏷️ Ticket Promedio", f"${avg_ticket:,.2f}")

        st.markdown("---")

        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("🏆 Top 10 Productos por Recaudación")
            top10_products = df_products.sort_values(by="total_revenue", ascending=False).head(10)
            top10_products = top10_products.sort_values(by="total_revenue", ascending=True)

            fig_prod = px.bar(
                top10_products,
                x="total_revenue",
                y="product_name",
                orientation="h",
                labels={"total_revenue": "Ingresos (USD)", "product_name": ""},
                color_discrete_sequence=["#1f77b4"]
            )
            fig_prod.update_layout(
                height=450,
                margin=dict(l=10, r=10, t=10, b=10),
                yaxis=dict(tickfont=dict(size=11))
            )
            st.plotly_chart(fig_prod, use_container_width=True)

        with col_right:
            st.subheader("🌍 Top 10 Ventas por País")
            top10_countries = df_countries.sort_values(by="total_revenue", ascending=False).head(10)

            fig_country = px.bar(
                top10_countries,
                x="country",
                y="total_revenue",
                labels={"total_revenue": "Ingresos (USD)", "country": "País"},
                color_discrete_sequence=["#2ca02c"]
            )
            fig_country.update_layout(
                height=450,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis=dict(tickangle=-30)
            )
            st.plotly_chart(fig_country, use_container_width=True)

        st.markdown("---")

        st.subheader("📋 Resumen Geográfico Consolidado")
        df_table = df_countries.sort_values(by="total_revenue", ascending=False).copy()
        
        st.dataframe(
            df_table[['country', 'total_revenue', 'unique_customers', 'total_orders', 'avg_order_value']]
            .rename(columns={
                'country': 'País',
                'total_revenue': 'Ingresos Totales ($)',
                'unique_customers': 'Clientes Únicos',
                'total_orders': 'Total Órdenes',
                'avg_order_value': 'Ticket Promedio ($)'
            })
            .style.format({
                'Ingresos Totales ($)': '${:,.2f}',
                'Clientes Únicos': '{:,}',
                'Total Órdenes': '{:,}',
                'Ticket Promedio ($)': '${:,.2f}'
            }),
            use_container_width=True
        )

    # ==========================================
    # PESTAÑA 2: SIMULADOR STREAMING EN VIVO
    # ==========================================
    with tab_streaming:
        st.subheader("⚡ Monitor de Ingesta y Micro-Batches en Tiempo Real")
        st.caption("Simula y visualiza el flujo de streaming hacia las capas Bronze, Silver y Gold de Delta Lake.")

        col_ctl1, col_ctl2 = st.columns([1, 3])
        with col_ctl1:
            total_batches = st.slider("Número de Micro-Lotes a simular:", min_value=3, max_value=15, value=5)
            batch_delay = st.slider("Latencia entre lotes (segundos):", min_value=0.5, max_value=3.0, value=1.0, step=0.5)
            start_streaming = st.button("🚀 Iniciar Ingesta Streaming", use_container_width=True)

        status_box = st.empty()
        progress_bar = st.progress(0)

        kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
        m_batches = kpi_col1.empty()
        m_rows = kpi_col2.empty()
        m_speed = kpi_col3.empty()
        m_state = kpi_col4.empty()

        log_expander = st.expander("📝 Terminal de Logs de Ingesta (Spark Structured Streaming)", expanded=True)
        log_box = log_expander.empty()

        if start_streaming:
            accumulated_rows = 0
            log_history = []
            
            for b in range(1, total_batches + 1):
                batch_rows = len(df_products) * (b * 120)
                accumulated_rows += batch_rows
                current_time = pd.Timestamp.now().strftime("%H:%M:%S")

                log_entry = f"[{current_time}] [BATCH {b:02d}/{total_batches:02d}] Ingestando micro-batch a Bronze ➔ Validación de Schema ➔ Escribiendo a Silver & Gold Delta Tables (+{batch_rows:,} registros)"
                log_history.insert(0, log_entry)

                m_batches.metric("📦 Lotes Procesados", f"{b} / {total_batches}")
                m_rows.metric("📈 Filas Ingeridas", f"{accumulated_rows:,}")
                m_speed.metric("⚡ Velocidad", f"{int(batch_rows/batch_delay):,} ops/s")
                m_state.metric("🟢 Estado del Pipeline", "RUNNING")

                progress_bar.progress(int((b / total_batches) * 100))
                status_box.info(f"Procesando Micro-Lote #{b} en Delta Lake...")
                log_box.code("\n".join(log_history), language="bash")

                time.sleep(batch_delay)

            m_state.metric("🟢 Estado del Pipeline", "COMPLETED")
            status_box.success("🎉 ¡Ingesta de streaming completada con éxito! Todas las capas Delta están sincronizadas.")
        else:
            m_batches.metric("📦 Lotes Procesados", "0 / 0")
            m_rows.metric("📈 Filas Ingeridas", "0")
            m_speed.metric("⚡ Velocidad", "0 ops/s")
            m_state.metric("⚪ Estado del Pipeline", "IDLE")
            log_box.info("Presiona 'Iniciar Ingesta Streaming' para observar la ingesta de micro-lotes en tiempo real.")

    # ==========================================
    # PESTAÑA 3: AGENTE IA (TEXT-TO-SQL CON GEMINI)
    # ==========================================
    with tab_agent:
        col_ag_title, col_ag_btn = st.columns([4, 1])
        with col_ag_title:
            st.subheader("🤖 Asistente de Analítica del Lakehouse (Powered by Gemini)")
            st.caption("Haz preguntas en lenguaje natural. Gemini interpretará tu consulta, generará SQL sobre DuckDB y te devolverá el análisis financiero.")
        with col_ag_btn:
            if st.button("🗑️ Limpiar Chat", use_container_width=True):
                st.session_state.messages = [
                    {"role": "assistant", "content": "¡Hola! Soy tu asistente de analítica del Lakehouse. Puedes preguntarme sobre ventas, ticket promedio, países o productos más vendidos."}
                ]
                st.rerun()

        if "messages" not in st.session_state:
            st.session_state.messages = [
                {"role": "assistant", "content": "¡Hola! Soy tu asistente de analítica del Lakehouse. Puedes preguntarme sobre ventas, ticket promedio, países o productos más vendidos."}
            ]

        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        if prompt := st.chat_input("Escribe tu pregunta analítica aquí..."):
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)

            if not client_gemini:
                st.error("No se encontró `GEMINI_API_KEY` en los Secrets/Entorno. Configúrala para activar el asistente.")
            else:
                with st.chat_message("assistant"):
                    with st.spinner("Analizando esquemas y consultando Delta Lake con Gemini..."):
                        try:
                            clean_sql, query_result_df, final_answer = process_agent_query(prompt)

                            # Desplegar respuesta + SQL auditable
                            st.markdown(final_answer)
                            with st.expander("🔍 Ver consulta SQL generada y resultado"):
                                st.code(clean_sql, language="sql")
                                st.dataframe(query_result_df, use_container_width=True)

                            st.session_state.messages.append({"role": "assistant", "content": final_answer})

                        except Exception as err:
                            error_msg = f"Hubo un detalle al procesar la consulta: {err}"
                            st.error(error_msg)
                            st.session_state.messages.append({"role": "assistant", "content": error_msg})

else:
    st.warning("Cargando datos desde Delta Lake en S3... Verifica tus credenciales y conexión.")