"""
Вывод из view_result.ipynb


Читаем последний отчет из: ./output_task2/report_20260110_232300

==============================
ИТОГОВЫЙ ТОП-10 СЛОВ:
==============================
+------------+-----+
|cleaned_word|count|
+------------+-----+
|Пайор       |4    |
|Челс        |4    |
|Эв          |4    |
|Атлетик     |4    |
|Тв          |3    |
|Доз         |3    |
|Футбол      |3    |
|Мадрид      |3    |
|Вот         |2    |
|Эт          |2    |
+------------+-----+
"""


import os
import re
import shutil
import time
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, desc, explode, split, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# настройки для macOS
os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
# Путь к Java
java_home = "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
if os.path.exists(java_home):
    os.environ["JAVA_HOME"] = java_home

# конфигурация
KAFKA_SERVER = "localhost:9092"
TOPIC = "telegram_stream"
OUTPUT_DIR = "./output_task2"
CHECKPOINT_DIR = "./checkpoint_task2"

# Автоматически определяем версию Spark для скачивания коннектора Kafka
spark_version = pyspark.__version__
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}"
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {kafka_package} pyspark-shell'

# ЛОГИКА ОЧИСТКИ (Задание 2)
# Оставляем слова с большой буквы (кириллица), убираем гласные в конце
def clean_word_func(word):
    if not word: return None
    # 1. Проверка: только кириллица, начинается с Заглавной
    if not re.match(r'^[А-ЯЁ][а-яё]+$', word):
        return None
    
    # 2. Удаление гласных и й в конце (по заданию)
    cleaned = re.sub(r'[аеёиоуыэюяйАЕЁИОУЫЭЮЯЙ]+$', '', word)
    
    # 3. Если слово стало слишком коротким, выкидываем
    if len(cleaned) < 2: 
        return None
        
    return cleaned

clean_udf = udf(clean_word_func, StringType())

def main():
    # Очистка папок перед запуском
    if os.path.exists(OUTPUT_DIR): shutil.rmtree(OUTPUT_DIR)
    if os.path.exists(CHECKPOINT_DIR): shutil.rmtree(CHECKPOINT_DIR)
    
    # Инициализация Spark
    spark = SparkSession.builder \
        .appName("HW3_Task2_Kafka") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print(f">>> Spark запущен. Слушаю топик: {TOPIC}")

    # 1 Читаем Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2 Парсим JSON
    schema = StructType([
        StructField("text", StringType()),
        StructField("timestamp", DoubleType())
    ])
    
    df_text = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select(col("data.text"))

    # 3 Разбиваем на слова и чистим
    words_df = df_text \
        .select(explode(split(col("text"), "[^а-яА-ЯёЁ]+")).alias("raw_word")) \
        .withColumn("cleaned_word", clean_udf(col("raw_word"))) \
        .filter(col("cleaned_word").isNotNull())

    # 4 Считаем (Глобальная агрегация)
    counts = words_df.groupBy("cleaned_word").count()

    # Функция сохранения каждого батча
    def save_batch(batch_df, batch_id):
        if batch_df.isEmpty(): return
            
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Сохраняю статистику... (Batch {batch_id})")
        
        # Сортировка по убыванию
        sorted_df = batch_df.orderBy(desc("count"))
        
        # Сохранение в CSV
        save_path = f"{OUTPUT_DIR}/report_{ts}"
        sorted_df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(save_path)

    # 5 Запуск стрима
    query = counts.writeStream \
        .outputMode("complete") \
        .foreachBatch(save_batch) \
        .trigger(processingTime='1 minute') \
        .start()

    try:
        # Держим стрим запущенным
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n>>> Остановка...")
        query.stop()
        
        # Вывод финального Топ-10 при выходе
        print("\n" + "="*30)
        print("ИТОГОВЫЙ ТОП-10 СЛОВ:")
        print("="*30)
        if os.path.exists(OUTPUT_DIR):
            subdirs = [os.path.join(OUTPUT_DIR, d) for d in os.listdir(OUTPUT_DIR) if d.startswith("report")]
            if subdirs:
                latest_dir = max(subdirs, key=os.path.getmtime)
                final_df = spark.read.option("header", "true").csv(latest_dir)
                final_df.show(10, truncate=False)
        else:
            print("Данных накоплено не было")

if __name__ == "__main__":
    main()

