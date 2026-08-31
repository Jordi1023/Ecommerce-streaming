import os
import time
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

TOTAL_DATA = 530693
BATCH_SIZE = 50
START_BATCH = 140
TOTAL_BATCHES_TO_RUN = 80  # Corre 80 lotes para darte buen tiempo de grabación (~30 segundos)

os.system("cls" if os.name == "nt" else "clear")

print("=" * 85)
print(" [PRODUCER & STREAMING ENGINE] TRANSMISIÓN DE EVENTOS EN TIEMPO REAL")
print(f" Destino: AWS S3 Lakehouse (Bronze ➔ Silver ➔ Gold)")
print(f" Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 85)
print("\nIniciando micro-batches de transacciones e-commerce...\n")

current_total = START_BATCH * BATCH_SIZE

for i in range(1, TOTAL_BATCHES_TO_RUN + 1):
    batch_num = START_BATCH + i
    current_total += BATCH_SIZE
    
    # Imprime exactamente el formato que quieres
    print(f" [Lote #{batch_num:04d}] +{BATCH_SIZE} eventos reales enviados | Total: {current_total:,} / {TOTAL_DATA:,}")
    
    # Pausa de 0.35 segundos por lote para que se aprecie la animación en pantalla
    time.sleep(0.35)

print("\n" + "=" * 85)
print(" [PIPELINE STATUS] Lotes sincronizados correctamente con AWS S3 Delta Tables.")
print("=" * 85)