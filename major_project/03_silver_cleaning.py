import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

builder = SparkSession.builder \
    .appName("learntrack_silver") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "4")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

learners = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "learners"))
courses = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "courses"))
enrolments = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "enrolment_activity"))

learners_clean = learners \
    .withColumn("registration_date", F.to_date("registration_date", "yyyy-MM-dd")) \
    .drop("ingested_at")

instructor_map = spark.createDataFrame([
    ("INS001", "Rajiv Sharma"),
    ("INS002", "Meera Nair"),
    ("INS003", "Arjun Patel"),
    ("INS004", "Sunita Rao"),
    ("INS005", "Deepak Joshi"),
    ("INS006", "Priya Singh"),
    ("INS007", "Vikas Mehta"),
    ("INS008", "Kavita Reddy"),
    ("INS009", "Suresh Gupta"),
    ("INS010", "Anjali Iyer"),
    ("INS011", "Rohit Sharma"),
    ("INS012", "Nisha Agarwal"),
    ("INS013", "Manish Verma"),
    ("INS014", "Pooja Mishra"),
    ("INS015", "Kiran Desai"),
], ["instructor_id", "name_lookup"])

courses_clean = courses \
    .join(instructor_map, on="instructor_id", how="left") \
    .withColumn(
        "instructor_name",
        F.when(F.col("instructor_name").isNull(), F.col("name_lookup"))
         .otherwise(F.col("instructor_name"))
    ) \
    .withColumn("duration_hours", F.col("duration_hours").cast("integer")) \
    .withColumn("price_inr", F.col("price_inr").cast("decimal(10,2)")) \
    .drop("name_lookup", "ingested_at")

still_blank = courses_clean.filter(F.col("instructor_name").isNull()).count()
print(f"Instructor names still blank after fix: {still_blank}")

enrolments_clean = enrolments \
    .withColumn("enrol_date", F.to_date("enrol_date", "yyyy-MM-dd")) \
    .withColumn("expected_completion_date", F.to_date("expected_completion_date", "yyyy-MM-dd")) \
    .withColumn("actual_completion_date", F.to_date("actual_completion_date", "yyyy-MM-dd")) \
    .withColumn("last_activity_date", F.to_date("last_activity_date", "yyyy-MM-dd")) \
    .withColumn("progress_pct", F.col("progress_pct").cast("integer")) \
    .withColumn("assessment_score", F.col("assessment_score").cast("decimal(5,2)")) \
    .withColumn("attempts", F.col("attempts").cast("integer")) \
    .withColumn("feedback_rating", F.col("feedback_rating").cast("integer")) \
    .drop("ingested_at")

before_count = enrolments_clean.count()
w = Window.partitionBy("enrolment_id").orderBy("enrol_date")
enrolments_clean = enrolments_clean \
    .withColumn("row_num", F.row_number().over(w)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")
after_count = enrolments_clean.count()
print(f"Rows before removing duplicates: {before_count}, after: {after_count}")

enrolments_clean = enrolments_clean.withColumn(
    "learning_duration_days",
    F.when(
        F.col("actual_completion_date").isNotNull(),
        F.datediff(F.col("actual_completion_date"), F.col("enrol_date"))
    ).otherwise(None)
)

silver_table = enrolments_clean \
    .join(learners_clean.alias("l"), on="learner_id", how="left") \
    .join(courses_clean.alias("c"), on="course_id", how="left") \
    .select(
        "enrolment_id", "learner_id", "course_id",
        "enrol_date", "expected_completion_date", "actual_completion_date",
        "status", "progress_pct", "last_activity_date",
        "assessment_score", "attempts", "feedback_rating",
        "certificate_issued", "learning_duration_days",
        F.col("l.learner_name"),
        F.col("l.email"),
        F.col("l.city"),
        F.col("l.subscription_type"),
        F.col("l.registration_date"),
        F.col("c.course_title"),
        F.col("c.category"),
        F.col("c.instructor_id"),
        F.col("c.instructor_name"),
        F.col("c.duration_hours"),
        F.col("c.difficulty_level"),
        F.col("c.price_inr"),
    )

out_path = os.path.join(SILVER_DIR, "silver_enrolments")
silver_table.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out_path)
print(f"Silver table saved: {silver_table.count()} rows")

silver_table.groupBy("status").count().orderBy("count", ascending=False).show()

spark.stop()
