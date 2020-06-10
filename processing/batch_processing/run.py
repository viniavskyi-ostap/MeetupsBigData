import os
import time

MASTER = "local[*]"

if __name__ == "__main__":
    while True:
        os.system(f"$SPARK_PATH/bin/spark-submit --master {MASTER} --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --py-files schema.py, batch_processing/processor.py")
        time.sleep(3600)
