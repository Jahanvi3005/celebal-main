import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from delta import configure_spark_with_delta_pip

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")

builder = SparkSession.builder \
    .appName("learntrack_bronze") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "4")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def load_to_bronze(file_name, table_name):
    src = os.path.join(RAW_DIR, file_name)
    dest = os.path.join(BRONZE_DIR, table_name)

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("nullValue", "") \
        .csv(src)

    df = df.withColumn("ingested_at", current_timestamp())

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(dest)
    print(f"Loaded {file_name} -> {table_name} ({df.count()} rows)")
    return df

df_learners = load_to_bronze("learners.csv", "learners")
df_courses = load_to_bronze("courses.csv", "courses")
df_enrolments = load_to_bronze("enrolment_activity.csv", "enrolment_activity")

from pyspark.sql.functions import count

dups = df_enrolments.groupBy("enrolment_id").agg(count("*").alias("cnt")).filter("cnt > 1").count()
print(f"Duplicate enrolment IDs found: {dups}")

blank_inst = df_courses.filter(col("instructor_name").isNull()).count()
print(f"Blank instructor names in courses: {blank_inst}")

blank_scores = df_enrolments.filter(col("assessment_score").isNull()).count()
print(f"Null assessment scores: {blank_scores}")

spark.stop()
