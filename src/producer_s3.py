import os
import json
import time
from datetime import datetime, timezone
import boto3
from dotenv import load_dotenv
try:
    from src.generator import generate_ecommerce_event
except ModuleNotFoundError:
    from generator import generate_ecommerce_event

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
    """Sube un micro-lote de eventos a S3 particionado por anio/mes/dia/hora."""
    now = datetime.now(timezone.utc)
    partition_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
    file_name = f"events_{now.strftime('%Y%m%d_%H%M%S')}_{len(events)}_records.json"
    s3_key = f"raw_landing/ecommerce_stream/{partition_path}/{file_name}"
    
    # Formato NDJSON (Newline Delimited JSON) - estandar para Spark / Databricks
    payload = "\n".join([json.dumps(e) for e in events])
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=payload.encode("utf-8"),
        ContentType="application/json"
    )
    return s3_key

def run_continuous_producer(batch_size: int = 25, interval_seconds: float = 3.0, max_batches: int = 10):
    """Ejecuta la simulacion de streaming enviando micro-lotes continuos."""
    print(f"🚀 Iniciando Streaming Producer hacia S3: {BUCKET_NAME}")
    print(f"Configuracion: {batch_size} eventos por lote cada {interval_seconds}s (Total: {max_batches} lotes)\n")
    
    for i in range(1, max_batches + 1):
        batch = [generate_ecommerce_event() for _ in range(batch_size)]
        s3_path = push_micro_batch_to_s3(batch)
        print(f"[{i}/{max_batches}] ✅ Lote subido: s3://{BUCKET_NAME}/{s3_path}")
        time.sleep(interval_seconds)
        
    print("\n🏁 Simulacion de streaming completada con exito.")

if __name__ == "__main__":
    run_continuous_producer(batch_size=20, interval_seconds=2.0, max_batches=5)