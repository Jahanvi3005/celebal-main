import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")
SQL_FILE = os.path.join(BASE_DIR, "05_sql_analytics.sql")

builder = SparkSession.builder \
    .appName("learntrack_sql_runner") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "4")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

tables = {
    "silver_enrolments": os.path.join(SILVER_DIR, "silver_enrolments"),
    "gold_course_completion": os.path.join(GOLD_DIR, "gold_course_completion"),
    "gold_learner_engagement": os.path.join(GOLD_DIR, "gold_learner_engagement"),
    "gold_instructor_effectiveness": os.path.join(GOLD_DIR, "gold_instructor_effectiveness"),
    "gold_assessment_performance": os.path.join(GOLD_DIR, "gold_assessment_performance"),
    "gold_dropout_reenrolment": os.path.join(GOLD_DIR, "gold_dropout_reenrolment")
}

for view_name, path in tables.items():
    if os.path.exists(path):
        df = spark.read.format("delta").load(path)
        df.createOrReplaceTempView(view_name)

with open(SQL_FILE, "r") as f:
    sql_content = f.read()

raw_queries = sql_content.split(";")
queries_to_run = []

for q in raw_queries:
    query_str = q.strip()
    if query_str:
        queries_to_run.append(query_str)

output_report_path = os.path.join(BASE_DIR, "sql_analytics_results.txt")
with open(output_report_path, "w") as out_file:
    for query in queries_to_run:
        try:
            res_df = spark.sql(query)
            # convert to pandas to format without grid lines
            pdf = res_df.toPandas()
            result_str = pdf.to_string(index=False)
            out_file.write(result_str)
            out_file.write("\n\n")
        except Exception as e:
            pass

spark.stop()
