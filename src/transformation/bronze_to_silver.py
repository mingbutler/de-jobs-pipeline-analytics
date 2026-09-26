from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from databricks.sdk.runtime import dbutils

from keyword_extractor import add_education_level_column, add_experience_years_column, add_skills_column

spark = DatabricksSession.builder.getOrCreate()

TABLE_NAME = "workspace.silver.job_postings"
VOLUME_PATH = "dbfs:/Volumes/workspace/bronze/raw_data/"

raw_df = spark.read.json(VOLUME_PATH, multiLine=True)

# explode nested data
jobs_df = raw_df.select(F.explode('data.jobs').alias('job'))

# flatten and select needed fields 
silver_df = jobs_df.select(
    F.col("job.job_id").alias("job_id"),
    F.col("job.job_title").alias("job_title"),
    F.col("job.employer_name").alias("employer_name"),
    F.col("job.employer_logo").alias("employer_logo"),
    F.col("job.job_employment_type").alias("employment_type"),
    F.col("job.job_is_remote").cast("boolean").alias("is_remote"),
    F.col("job.job_city").alias("city"),
    F.col("job.job_state").alias("state"),
    F.col("job.job_country").alias("country"),
    F.col("job.job_min_salary").cast("double").alias("min_salary"),
    F.col("job.job_max_salary").cast("double").alias("max_salary"),
    F.col("job.job_salary_period").alias("salary_period"),
    F.col("job.job_description").alias("job_description"),
    F.from_unixtime(F.col("job.job_posted_at_timestamp")).cast("timestamp").alias("posted_at"),
    F.col("job.job_publisher").alias("publisher"),
    F.col("job.job_apply_link").alias("apply_link"),
)

# drop duplicate jobs
silver_df = silver_df.dropDuplicates(["job_id"])
# remove rows with no job_id
silver_df = silver_df.filter(F.col('job_id').isNotNull()).withColumn('_ingested_at', F.current_timestamp())

# add skills, experience years, and education keyword column
silver_df = add_skills_column(silver_df, text_col="job_description")
silver_df = add_experience_years_column(silver_df, text_col="job_description")
silver_df = add_education_level_column(silver_df, text_col="job_description")

# write to delta table
# merge new data logic
if spark.catalog.tableExists(TABLE_NAME):
    target = DeltaTable.forName(spark, TABLE_NAME)
    (target.alias('t')
        .merge(silver_df.alias('s'), "t.job_id = s.job_id") 
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    silver_df.write.format('delta').saveAsTable('workspace.silver.job_postings')
    
# pull insert/update counts from merge metrics
metrics = spark.sql(f"DESCRIBE HISTORY {TABLE_NAME} LIMIT 1").collect()[0]['operationMetrics']
rows_changed = int(metrics.get('numTargetRowsInserted', 0)) + int(metrics.get('numTargetRowsUpdated', 0))

dbutils.jobs.taskValues.set(key="silver_rows_changed", value="true" if rows_changed > 0 else "false")