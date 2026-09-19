from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

spark = DatabricksSession.builder.getOrCreate()

volume_path = "dbfs:/Volumes/workspace/bronze/raw_data/"

# databricks auto loader
raw_df = spark.read.json(volume_path, multiLine=True)

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

# write to delta table
silver_df.write.format('delta').mode('overwrite').saveAsTable('workspace.silver.job_postings')