import os
import urllib.request
import pandas as pd

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
FILE_PATH = os.path.join(DATA_DIR, "ecommerce_real_sample.csv")

def download_sample_data():
    """Descarga un dataset real de transacciones de e-commerce para streaming playback."""
    # Dataset publico de compras online reales (UCI / Kaggle Retail Dataset)
    url = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/all/online-retail-dataset.csv"
    
    print("📥 Descargando dataset real de transacciones...")
    if not os.path.exists(FILE_PATH):
        urllib.request.urlretrieve(url, FILE_PATH)
        print(f"✅ Archivo descargado en: {FILE_PATH}")
    else:
        print(f"ℹ️ El archivo ya existe en: {FILE_PATH}")
        
    df = pd.read_csv(FILE_PATH)
    print(f"📊 Dataset cargado con éxito: {len(df):,} transacciones reales listas para emitir.")
    print(df.head(3))

if __name__ == "__main__":
    download_sample_data()