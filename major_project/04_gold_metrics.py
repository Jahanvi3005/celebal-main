import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

builder = SparkSession.builder \
    .appName("learntrack_gold") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "4")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver = spark.read.format("delta").load(os.path.join(SILVER_DIR, "silver_enrolments"))

def write_gold(df, name):
    path = os.path.join(GOLD_DIR, name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    print(f"Saved {name} ({df.count()} rows)")

course_completion = silver.groupBy(
    "course_id", "course_title", "category", "difficulty_level", "instructor_name"
).agg(
    F.count("enrolment_id").alias("total_enrolled"),
    F.sum(F.when(F.col("status") == "Completed", 1).otherwise(0)).alias("total_completed"),
    F.sum(F.when(F.col("status") == "Dropped", 1).otherwise(0)).alias("total_dropped"),
    F.round(F.avg("assessment_score"), 2).alias("avg_assessment_score"),
    F.round(F.avg("learning_duration_days"), 2).alias("avg_completion_days"),
).withColumn(
    "completion_rate_pct",
    F.round((F.col("total_completed") / F.col("total_enrolled")) * 100, 2)
).withColumn(
    "dropout_rate_pct",
    F.round((F.col("total_dropped") / F.col("total_enrolled")) * 100, 2)
).withColumn(
    "completion_flag",
    F.when(F.col("completion_rate_pct") >= 60, "High Completion")
     .when(F.col("completion_rate_pct") >= 40, "Moderate")
     .otherwise("At Risk")
).orderBy("completion_rate_pct", ascending=False)

write_gold(course_completion, "gold_course_completion")
course_completion.show(5, truncate=35)

max_date = silver.select(F.max("last_activity_date")).collect()[0][0]

learner_engagement = silver.filter(
    F.col("status").isin("In Progress", "Not Started")
).withColumn(
    "days_since_activity",
    F.when(F.col("last_activity_date").isNotNull(),
           F.datediff(F.lit(max_date), F.col("last_activity_date")))
     .otherwise(F.lit(999))
).withColumn(
    "engagement_status",
    F.when(F.col("days_since_activity") <= 7, "Active")
     .when(F.col("days_since_activity") <= 30, "At Risk")
     .otherwise("Disengaged")
).select(
    "learner_id", "learner_name", "city", "subscription_type",
    "course_id", "course_title", "category",
    "status", "progress_pct", "last_activity_date",
    "days_since_activity", "engagement_status", "enrol_date"
).orderBy("days_since_activity", ascending=False)

write_gold(learner_engagement, "gold_learner_engagement")
learner_engagement.groupBy("engagement_status").count().show()

per_course = silver.groupBy(
    "instructor_id", "instructor_name", "course_id", "course_title", "category"
).agg(
    F.count("enrolment_id").alias("total_enrolled"),
    F.sum(F.when(F.col("status") == "Completed", 1).otherwise(0)).alias("total_completed"),
    F.avg("assessment_score").alias("avg_score"),
    F.avg("feedback_rating").alias("avg_feedback"),
).withColumn(
    "course_completion_rate",
    F.round((F.col("total_completed") / F.col("total_enrolled")) * 100, 2)
)

instructor_summary = per_course.groupBy("instructor_id", "instructor_name").agg(
    F.count("course_id").alias("total_courses"),
    F.sum("total_enrolled").alias("total_learners"),
    F.round(F.avg("course_completion_rate"), 2).alias("avg_completion_rate_pct"),
    F.round(F.avg("avg_score"), 2).alias("avg_assessment_score"),
    F.round(F.avg("avg_feedback"), 2).alias("avg_feedback_rating"),
)

w_rank = Window.orderBy(F.col("avg_completion_rate_pct").desc())
instructor_effectiveness = instructor_summary.withColumn(
    "rank_by_completion", F.dense_rank().over(w_rank)
).withColumn(
    "performance_tier",
    F.when(F.col("avg_completion_rate_pct") >= 55, "Top Performer")
     .when(F.col("avg_completion_rate_pct") >= 40, "Average")
     .otherwise("Needs Review")
).orderBy("rank_by_completion")

write_gold(instructor_effectiveness, "gold_instructor_effectiveness")
instructor_effectiveness.show(5, truncate=30)

assessment_perf = silver.filter(
    F.col("assessment_score").isNotNull()
).groupBy(
    "course_id", "course_title", "category", "difficulty_level", "instructor_name"
).agg(
    F.count("enrolment_id").alias("total_assessed"),
    F.round(F.avg("assessment_score"), 2).alias("avg_score"),
    F.round(F.min("assessment_score"), 2).alias("min_score"),
    F.round(F.max("assessment_score"), 2).alias("max_score"),
    F.sum(F.when(F.col("assessment_score") >= 60, 1).otherwise(0)).alias("total_passed"),
    F.round(F.avg("attempts"), 2).alias("avg_attempts"),
).withColumn(
    "pass_rate_pct",
    F.round((F.col("total_passed") / F.col("total_assessed")) * 100, 2)
).withColumn(
    "difficulty_flag",
    F.when(F.col("pass_rate_pct") < 50, "High Difficulty - Bottleneck")
     .when(F.col("pass_rate_pct") < 70, "Moderate Difficulty")
     .otherwise("Accessible")
).orderBy("pass_rate_pct")

write_gold(assessment_perf, "gold_assessment_performance")
assessment_perf.show(5, truncate=35)

w_latest = Window.partitionBy("learner_id", "course_id").orderBy(F.col("enrol_date").desc())

dropout_reenrolment = silver.withColumn(
    "rn", F.row_number().over(w_latest)
).filter(F.col("rn") == 1).drop("rn").withColumn(
    "is_reenrolment",
    F.when(F.col("attempts") >= 2, "Yes").otherwise("No")
).withColumn(
    "learner_outcome",
    F.when(F.col("status") == "Completed", "Completed")
     .when(F.col("status") == "Dropped", "Dropped - At Risk")
     .when(F.col("status") == "In Progress", "In Progress")
     .otherwise("Not Started")
).select(
    "learner_id", "learner_name", "city", "subscription_type",
    "course_id", "course_title", "category",
    "enrol_date", "status", "progress_pct",
    "attempts", "is_reenrolment", "learner_outcome",
    "assessment_score", "certificate_issued"
).orderBy("is_reenrolment", ascending=False)

write_gold(dropout_reenrolment, "gold_dropout_reenrolment")

dropout_reenrolment.groupBy("is_reenrolment").count().show()
dropout_reenrolment.groupBy("learner_outcome").count().orderBy("count", ascending=False).show()

spark.stop()
