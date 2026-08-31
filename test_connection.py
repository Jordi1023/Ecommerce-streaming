import os
import boto3
from dotenv import load_dotenv

# Forzar recarga de variables del archivo .env
load_dotenv(override=True)

aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
aws_region = os.getenv("AWS_REGION", "").strip()
bucket_name = os.getenv("S3_BUCKET_NAME", "").strip()

print("--- Diagnóstico de Variables ---")
print(f"Access Key ID cargada: '{aws_access_key}' (Longitud: {len(aws_access_key)})")
print(f"Secret Key cargada: {'*' * (len(aws_secret_key)-4) + aws_secret_key[-4:] if len(aws_secret_key) >= 4 else 'VACÍA'} (Longitud: {len(aws_secret_key)})")
print(f"Región cargada: '{aws_region}'")
print(f"Bucket cargado: '{bucket_name}'")
print("--------------------------------\n")

if len(aws_access_key) != 20:
    print("⚠️ Advertencia: Un Access Key ID de AWS normalmente tiene exactamente 20 caracteres.")

if len(aws_secret_key) != 40:
    print(f"⚠️ Advertencia: Un Secret Access Key de AWS normalmente tiene exactamente 40 caracteres (la tuya tiene {len(aws_secret_key)}).")

try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    
    # Intentar subir el archivo de prueba
    s3_client.put_object(
        Bucket=bucket_name,
        Key="raw_landing/test_connection.txt",
        Body=b"Pipeline Ecommerce Streaming - Conexion AWS S3 exitosa!"
    )
    print(f"✅ ¡Conexión exitosa! Archivo creado en: s3://{bucket_name}/raw_landing/test_connection.txt")
except Exception as e:
    print(f"❌ Error al conectar con S3: {e}")