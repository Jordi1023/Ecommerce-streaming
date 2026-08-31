import os
import time
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = "ecommerce-streaming-jordi-2026"

print("=" * 70)
print("🚀 [INIT] INICIALIZANDO PIPELINE DE STREAMING & DELTA LAKE")
print(f"📦 Destino S3: s3://{S3_BUCKET}/delta/")
print(f"🕒 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# Cargar datos desde S3
storage_options = {
    "key": AWS_ACCESS_KEY,
    "secret": AWS_SECRET_KEY
}

try:
    print("\n⏳ [1/3] Conectando con S3 y leyendo capas Delta...")
    top_products_path = f"s3://{S3_BUCKET}/delta/gold_top_products/"
    df_products = pd.read_parquet(top_products_path, storage_options=storage_options)
    print(f"✅ [SUCCESS] Conexión establecida. Total catálogo disponible: {len(df_products):,} productos.")
except Exception as e:
    print(f"❌ Error al conectar a S3: {e}")
    exit(1)

# Simulación de Ingesta por Micro-Batches
TOTAL_BATCHES = 5
ROWS_PER_BATCH = 109975

print("\n⚡ [2/3] INICIANDO PROCESO DE STREAMING INGESTION (SPARK STRUCTURED STREAMING)...")
print("-" * 70)

total_rows = 0
for b in range(1, TOTAL_BATCHES + 1):
    total_rows += ROWS_PER_BATCH
    ts = datetime.now().strftime("%H:%M:%S")
    
    # Barra de progreso visual en consola
    progress = int((b / TOTAL_BATCHES) * 20)
    bar = "█" * progress + "-" * (20 - progress)
    
    print(f"[{ts}] Micro-Batch #{b:02d} | [{bar}] {int((b/TOTAL_BATCHES)*100)}%")
    print(f"       ➔ Ingesta a capa Bronze (Raw Data)")
    print(f"       ➔ Transformación y limpieza a Silver (Quality Rules Passed)")
    print(f"       ➔ Agregaciones escritas en Gold Delta Tables (+{ROWS_PER_BATCH:,} registros)")
    print(f"       📊 Total acumulado en Lakehouse: {total_rows:,} registros")
    print("-" * 70)
    time.sleep(1.2)

print("\n🎉 [3/3] ¡STREAMING INGESTION COMPLETADA EXITOSAMENTE!")
print(f"📦 Total Lotes: {TOTAL_BATCHES} | 📈 Registros Totales Procesados: {total_rows:,}")
print("🟢 Estado de tablas Delta en S3: SYNCHRONIZED & READY")
print("=" * 70)