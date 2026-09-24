from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

spark = DatabricksSession.builder.getOrCreate()

silver_df = spark.table('workspace.silver.job_postings')

# gold layer skills demand
skills_df = silver_df.withColumn('skill', F.explode(F.col('skills')))

gold_skill_demand = skills_df.groupBy('skill').agg(F.count('*').alias('posting_count')).orderBy(F.desc('posting_count'))

# gold layer experience requirements
experience_range = silver_df.withColumn('experience_range',
    F.when(F.col('extracted_min_years_experience') <= 2.0, '0-2')
     .when(F.col('extracted_min_years_experience') <= 5.0, '3-5')
     .when(F.col('extracted_min_years_experience') <= 8.0, '5-8')
     .when(F.col('extracted_min_years_experience') >= 9.0, '10+')
     .otherwise('Not Found')
)

gold_experience = experience_range.groupBy('experience_range').count()

# postings by location and salary
locations = silver_df.dropna(subset=['state', 'city']).groupBy('state', 'city')

gold_location_salary = locations.agg(
    F.count('*').alias('posting_count'),
    F.round(F.avg('min_salary'), 2).alias('avg_min_salary'),
    F.round(F.avg('max_salary'), 2).alias('avg_max_salary')
).orderBy(F.desc('posting_count'))

# write to gold schema as separate tables
gold_dfs = [('skill_demand', gold_skill_demand),
            ('experience_requirements', gold_experience),
            ('location_salary_trend', gold_location_salary)]

for name, df in gold_dfs:
    df.write.format("delta").mode("overwrite").saveAsTable(f"workspace.gold.{name}")