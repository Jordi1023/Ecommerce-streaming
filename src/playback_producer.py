import os
import json
import time
import uuid
from datetime import datetime, timezone
import pandas as pd
import boto3
from dotenv import load_dotenv

load_dotenv(override=True)

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

def push_micro_batch_to_s3(events: list):
    now = datetime.now(timezone.utc)
    partition_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
    file_name = f"events_real_{now.strftime('%Y%m%d_%H%M%S_%f')}.json"
    s3_key = f"raw_landing/ecommerce_stream/{partition_path}/{file_name}"
    
    payload = "\n".join([json.dumps(e) for e in events])
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=payload.encode("utf-8"),
        ContentType="application/json"
    )
    return s3_key

def stream_real_dataset(batch_size: int = 50, sleep_seconds: float = 1.0):
    csv_path = os.path.join("data", "ecommerce_real_sample.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"No se encontró el dataset en {csv_path}. Ejecuta primero download_real_data.py")
        
    print(" Leyendo dataset real...")
    df = pd.read_csv(csv_path)
    
    # Limpieza inicial de nulos en identificadores clave
    df = df.dropna(subset=["InvoiceNo", "Description", "UnitPrice", "Quantity"])
    df = df[df["Quantity"] > 0]
    
    total_rows = len(df)
    print("=" * 65)
    print(f"🔴 PLAYBACK STREAMING ACTIVO: {total_rows:,} TRANSACCIONES REALES")
    print(f"Destino: s3://{BUCKET_NAME}")
    print(f"Emisión: {batch_size} eventos/segundo continuo")
    print("Presiona Ctrl + C para pausar la transmisión.")
    print("=" * 65 + "\n")
    
    total_sent = 0
    batch_num = 0
    
    try:
        for start_idx in range(0, total_rows, batch_size):
            batch_num += 1
            chunk = df.iloc[start_idx : start_idx + batch_size]
            events = []
            
            for _, row in chunk.iterrows():
                unit_price = float(row["UnitPrice"])
                qty = int(row["Quantity"])
                event = {
                    "event_id": str(uuid.uuid4()),
                    "order_id": f"INV-{row['InvoiceNo']}",
                    "user_id": f"USR-{int(row['CustomerID'])}" if pd.notnull(row['CustomerID']) else "USR-GUEST",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "event_type": "purchase",
                    "product_name": str(row["Description"]).strip(),
                    "category": "Retail Merchandise",
                    "unit_price": unit_price,
                    "quantity": qty,
                    "total_amount": round(unit_price * qty, 2),
                    "country": str(row["Country"]).strip()
                }
                events.append(event)
                
            s3_path = push_micro_batch_to_s3(events)
            total_sent += len(events)
            
            print(f" [Lote #{batch_num:04d}] +{len(events)} eventos reales enviados | Total: {total_sent:,} / {total_rows:,}")
            time.sleep(sleep_seconds)
            
    except KeyboardInterrupt:
        print(f"\n⏹️ Streaming pausado. Total registros emitidos: {total_sent:,}")

if __name__ == "__main__":
    stream_real_dataset(batch_size=50, sleep_seconds=1.0)
