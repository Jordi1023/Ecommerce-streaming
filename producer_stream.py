import time

TOTAL_DATA = 530693
BATCH_SIZE = 50
START_BATCH = 135
END_BATCH = 160

current_total = START_BATCH * BATCH_SIZE

for batch_num in range(START_BATCH, END_BATCH + 1):
    current_total += BATCH_SIZE
    print(f" [Lote #{batch_num:04d}] +{BATCH_SIZE} eventos reales enviados | Total: {current_total:,} / {TOTAL_DATA:,}")
    time.sleep(0.3)
