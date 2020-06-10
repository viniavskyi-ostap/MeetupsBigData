import os
import time

MASTER = "spark://ip-172-31-65-164.ec2.internal:7077"

if __name__ == "__main__":
    while True:
        os.system(f"$SPARK_PATH/bin/spark-submit --master {MASTER} --conf 'spark.blockManager.port=10025' --conf 'spark.driver.port=10026' --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --py-files schema.py, batch_processing/processor.py")
        time.sleep(3600)
