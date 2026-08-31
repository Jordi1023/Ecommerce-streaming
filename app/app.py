import os
import time
import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb
from google import genai
from dotenv import load_dotenv

# =============================================================================
# 1. CONFIGURACIÓN DE PÁGINA Y VARIABLES DE ENTORNO
# =============================================================================
st.set_page_config(
    page_title="E-Commerce Lakehouse & AI Agent",
    page_icon="🛒",
    layout="wide"
)

# Cargar variables locales desde .env si existe
load_dotenv()

def get_config_val(key, default=None):
    """Obtiene variables desde st.secrets (Cloud) o variables de entorno (Local)."""
    if key in st.secrets:
        return st.secrets[key]
    return os.getenv(key, default)

AWS_ACCESS_KEY = get_config_val("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = get_config_val("AWS_SECRET_ACCESS_KEY")
AWS_REGION = get_config_val("AWS_DEFAULT_REGION", "us-east-2")
S3_BUCKET = get_config_val("S3_BUCKET", "ecommerce-streaming-jordi-2026")
GEMINI_API_KEY = get_config_val("GEMINI_API_KEY")

# Inicializar cliente oficial de Gemini
client_gemini = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

# =============================================================================
# 2. MOTOR DEL AGENTE ANALÍTICO (TEXT-TO-SQL + GENAI + FALLBACK LOCAL)
# =============================================================================
def process_agent_query(user_query, max_retries=2):
    """
    Agente de IA Resiliente:
    - Interpreta solicitudes analíticas, conversacionales, compuestas o de humor.
    - Ejecuta Text-to-SQL en DuckDB in-memory.
    - Si la cuota de la API se agota o hay latencia, conmuta al motor heurístico local.
    """
    system_prompt = f"""
Eres un Lead Data Analyst y especialista en SQL ANSI para un Lakehouse de E-Commerce (+530k transacciones en DuckDB).
Eres profesional, empático y atiendes solicitudes compuestas (chistes, saludos o aclaraciones) con naturalidad.

Tablas disponibles en DuckDB:
1. `df_products`:
   - product_name (VARCHAR): Nombre del producto
   - total_revenue (DOUBLE): Facturación acumulada en USD
   - units_sold (BIGINT): Unidades vendidas
   - total_orders (BIGINT): Cantidad de órdenes donde aparece el producto

2. `df_countries`:
   - country (VARCHAR): Nombre del país
   - total_revenue (DOUBLE): Facturación acumulada en USD
   - unique_customers (BIGINT): Total de clientes únicos
   - total_orders (BIGINT): Total de órdenes
   - avg_order_value (DOUBLE): Ticket promedio en USD

Pregunta del usuario: "{user_query}"

INSTRUCCIONES DE RESPUESTA:
1. Si el usuario pide un chiste, saludo o comentario casual ADEMÁS o EN LUGAR de datos, responde amablemente a todo en el análisis.
2. Si la pregunta requiere consultar la base de datos, genera una consulta SQL ANSI válida para DuckDB.
3. Si la pregunta es 100% conversacional y NO requiere datos de las tablas, escribe la palabra 'NONE' en el bloque SQL.
4. Responde ÚNICAMENTE en este formato separado por '---SPLIT---':

[SQL PLANO O LA PALABRA NONE]
---SPLIT---
[ANÁLISIS EJECUTIVO / RESPUESTA COMPLETA EN MARKDOWN]
"""

    VALID_MODELS = ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-1.5-flash']
    api_error = None

    # Intento con API de Gemini
    if client_gemini:
        for model_name in VALID_MODELS:
            for attempt in range(max_retries):
                try:
                    response = client_gemini.models.generate_content(
                        model=model_name,
                        contents=system_prompt,
                    )
                    full_text = response.text.strip()

                    if "---SPLIT---" in full_text:
                        parts = full_text.split("---SPLIT---")
                        sql_query = parts[0].replace("```sql", "").replace("```", "").strip()
                        interpretation = parts[1].strip()
                    else:
                        sql_query = "NONE"
                        interpretation = full_text

                    if sql_query and sql_query != "NONE" and "SELECT" in sql_query.upper():
                        result_df = duckdb.query(sql_query).df()
                    else:
                        sql_query = "-- Consulta conversacional (sin consulta a base de datos)"
                        result_df = pd.DataFrame()

                    return sql_query, result_df, interpretation

                except Exception as e:
                    api_error = str(e)
                    if ("429" in api_error or "RESOURCE_EXHAUSTED" in api_error or "503" in api_error) and attempt < max_retries - 1:
                        time.sleep(1.5)
                        continue
                    break

    # =========================================================================
    # MOTOR DE RESPALDO LOCAL INTELIGENTE (OFFLINE / FALLO DE CUOTA)
    # =========================================================================
    q_lower = user_query.lower().strip()
    joke_prefix = ""
    if "chiste" in q_lower or "broma" in q_lower:
        joke_prefix = "😄 *¿Qué le dice una base de datos a otra? — 'Oye, ¿te paso una tabla o te da pereza indexar?'*\n\n---\n\n"

    # Clasificador de intención local
    if any(w in q_lower for w in ["producto", "articulo", "item", "mas vendido"]):
        fallback_sql = "SELECT product_name, units_sold, total_revenue FROM df_products ORDER BY total_revenue DESC LIMIT 5"
        res_df = duckdb.query(fallback_sql).df()
        top_name = res_df.iloc[0]['product_name']
        top_rev = res_df.iloc[0]['total_revenue']
        fallback_interp = f"{joke_prefix}### 🏆 Resumen Ejecutivo: Productos Principales\n\nEl producto con mayor recaudación es **{top_name}**, acumulando un total de **${top_rev:,.2f} USD**."
    
    elif any(w in q_lower for w in ["pais", "paises", "mercado", "geografia", "nacion"]):
        fallback_sql = "SELECT country, total_revenue, total_orders, avg_order_value FROM df_countries ORDER BY total_revenue DESC LIMIT 5"
        res_df = duckdb.query(fallback_sql).df()
        top_c = res_df.iloc[0]['country']
        top_rev = res_df.iloc[0]['total_revenue']
        fallback_interp = f"{joke_prefix}### 🌍 Resumen Ejecutivo: Desempeño por País\n\nEl mercado líder en volumen transaccional es **{top_c}**, con una facturación acumulada de **${top_rev:,.2f} USD**."
    
    else:
        # Respuesta conversacional genérica
        fallback_sql = "-- Consulta conversacional"
        res_df = pd.DataFrame()
        fallback_interp = f"""{joke_prefix}### 🤖 Asistente Analítico del Lakehouse

Estoy conectado al Lakehouse transaccional de E-Commerce en AWS S3 y DuckDB (+530k registros).

Puedes consultarme sobre:
* **📊 Ventas y GMV Global.**
* **🌍 Países con mayor y menor facturación.**
* **🏆 Productos con más ingresos o unidades vendidas.**
* **🏷️ Ticket promedio y distribución de clientes.**
"""

    if api_error and ("429" in api_error or "RESOURCE_EXHAUSTED" in api_error):
        fallback_interp += "\n\n> ℹ️ *Respuesta procesada mediante el motor local DuckDB (cuota temporal del Free Tier de Gemini agotada).* "

    return fallback_sql, res_df, fallback_interp

# =============================================================================
# 3. CARGA DE DATOS DESDE AWS S3 (CAPA GOLD) CON CACHÉ
# =============================================================================
@st.cache_data(ttl=60)
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
        st.error(f"Error al conectar con Capa Gold en S3: {e}")
        return pd.DataFrame(), pd.DataFrame()

df_products, df_countries = load_gold_data()

# =============================================================================
# 4. INTERFAZ VISUAL (STREAMLIT DASHBOARD)
# =============================================================================
st.title("⚡ E-Commerce Streaming Lakehouse & AI Agent")
st.markdown("Plataforma analítica con **Databricks Medallion Architecture** y **Agente Conversacional Text-to-SQL (Gemini)**.")
st.markdown("---")

if not df_products.empty and not df_countries.empty:

    tab_dashboard, tab_streaming, tab_agent = st.tabs([
        "📊 Dashboard Ejecutivo", 
        "⚡ Pipeline Streaming en Vivo", 
        "🤖 Agente IA (Text-to-SQL)"
    ])

    # -------------------------------------------------------------------------
    # PESTAÑA 1: DASHBOARD EJECUTIVO
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # PESTAÑA 2: SIMULADOR STREAMING EN VIVO
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # PESTAÑA 3: AGENTE IA (TEXT-TO-SQL CON GEMINI)
    # -------------------------------------------------------------------------
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

            with st.chat_message("assistant"):
                with st.spinner("Analizando esquemas y consultando Delta Lake..."):
                    try:
                        clean_sql, query_result_df, final_answer = process_agent_query(prompt)

                        st.markdown(final_answer)
                        if not query_result_df.empty:
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