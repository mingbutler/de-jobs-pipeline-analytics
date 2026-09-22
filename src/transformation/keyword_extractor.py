from pyspark.sql import functions as F

SKILLS_DICTIONARY = {
 
    "Programming Languages": {
        "Python": ["python"],
        "SQL": ["sql"],
        "Java": ["java(?!script)"],
        "Scala": ["scala"],
        "Java/Scala": ["jvm"],
        "Bash/Shell": ["bash", "shell script(ing)?"],
        "Go": ["golang", r"\bgo\b(?=.*(developer|engineer|language|programming))"],
    },
 
    "Big Data / Processing Frameworks": {
        "Apache Spark": ["spark", "pyspark", "apache spark"],
        "Hadoop": ["hadoop", "hdfs"],
        "Hive": ["hive", "apache hive"],
        "Kafka": ["kafka", "apache kafka"],
        "Flink": ["flink", "apache flink"],
        "Storm": ["apache storm"],
        "Beam": ["apache beam", "dataflow"],
        "Presto/Trino": ["presto", "trino"],
        "MapReduce": ["mapreduce"],
    },
 
    "Cloud Platforms": {
        "AWS": ["aws", "amazon web services"],
        "Azure": ["azure", "microsoft azure"],
        "GCP": ["gcp", "google cloud platform", "google cloud"],
        "Databricks": ["databricks"],
        "Snowflake": ["snowflake"],
    },
 
    "AWS Services": {
        "S3": [r"\bs3\b"],
        "Redshift": ["redshift"],
        "EMR": [r"\bemr\b"],
        "Glue": ["aws glue", "glue etl"],
        "Lambda": ["aws lambda"],
        "Kinesis": ["kinesis"],
        "Athena": ["athena"],
    },
 
    "Azure Services": {
        "Azure Data Factory": ["azure data factory", "adf"],
        "Azure Synapse": ["synapse"],
        "Azure Data Lake": ["azure data lake", "adls"],
    },
 
    "GCP Services": {
        "BigQuery": ["bigquery"],
        "Google Cloud Storage": ["google cloud storage", r"\bgcs\b"],
        "Dataproc": ["dataproc"],
        "Pub/Sub": ["pub/sub", "pubsub"],
    },
 
    "Databases / Data Warehouses": {
        "PostgreSQL": ["postgres(ql)?"],
        "MySQL": ["mysql"],
        "SQL Server": ["sql server", "mssql"],
        "Oracle": ["oracle db", "oracle database"],
        "MongoDB": ["mongodb", "mongo db"],
        "Cassandra": ["cassandra"],
        "DynamoDB": ["dynamodb"],
        "Redis": ["redis"],
        "Elasticsearch": ["elasticsearch", "elastic search"],
        "Teradata": ["teradata"],
    },
 
    "Data Lake / Table Formats": {
        "Delta Lake": ["delta lake", "delta table"],
        "Apache Iceberg": ["iceberg"],
        "Apache Hudi": ["hudi"],
        "Parquet": ["parquet"],
        "Avro": ["avro"],
        "ORC": [r"\borc\b"],
    },
 
    "Orchestration / Workflow": {
        "Airflow": ["airflow", "apache airflow"],
        "dbt": [r"\bdbt\b"],
        "Prefect": ["prefect"],
        "Dagster": ["dagster"],
        "Luigi": ["luigi"],
        "Azure Data Factory (orchestration)": ["adf pipeline"],
        "Databricks Workflows": ["databricks workflows"],
    },
 
    "DevOps / CI-CD / Infra": {
        "Docker": ["docker", "containeriz(ed|ation)"],
        "Kubernetes": ["kubernetes", r"\bk8s\b"],
        "Terraform": ["terraform"],
        "Jenkins": ["jenkins"],
        "GitHub Actions": ["github actions"],
        "GitLab CI": ["gitlab ci"],
        "CI/CD": ["ci/cd", "continuous integration", "continuous deployment"],
        "Ansible": ["ansible"],
    },
 
    "Version Control": {
        "Git": [r"\bgit\b"],
        "GitHub": ["github"],
        "GitLab": ["gitlab"],
        "Bitbucket": ["bitbucket"],
    },
 
    "Data Modeling / Concepts": {
        "ETL/ELT": ["etl", "elt"],
        "Data Warehousing": ["data warehous(e|ing)"],
        "Data Modeling": ["data model(l)?ing", "dimensional model(l)?ing"],
        "Star Schema": ["star schema"],
        "Data Lake": ["data lake"],
        "Data Governance": ["data governance"],
        "Data Quality": ["data quality"],
        "Master Data Management": ["master data management", r"\bmdm\b"],
        "Medallion Architecture": ["medallion architecture", "bronze.{0,10}silver.{0,10}gold"],
    },
 
    "Streaming / Real-Time": {
        "Stream Processing": ["stream processing", "streaming data", "real-time processing"],
        "Change Data Capture": ["change data capture", r"\bcdc\b"],
    },
 
    "BI / Visualization": {
        "Tableau": ["tableau"],
        "Power BI": ["power bi", "powerbi"],
        "Looker": ["looker"],
        "Databricks SQL": ["databricks sql"],
    },
 
    "Other Tools / Libraries": {
        "Pandas": ["pandas"],
        "NumPy": ["numpy"],
        "Jupyter": ["jupyter"],
        "REST APIs": ["rest api", "restful api"],
        "Excel": ["excel", "microsoft excel"],
        "JIRA": ["jira"],
        "Agile/Scrum": ["agile", "scrum"],
    },
}

# experience extraction
_RANGE_PATTERNS = [
    r"(\d+)\s*(?:-|to)\s*(\d+)\s*\+?\s*years?",
]
 
_MIN_YEARS_PATTERNS = [
    r"(\d+)\s*\+\s*years?",                                     # "3+ years"
    r"minimum\s*(?:of\s*)?(\d+)\s*years?",                      # "minimum of 3 years"
    r"at\s*least\s*(\d+)\s*years?",                             # "at least 3 years"
    r"(\d+)\s*years?\s*(?:of\s*)?(?:relevant\s*)?experience",   # "3 years of experience"
    r"(\d+)\s*years?\s*in\s*(?:a\s*)?(?:related|similar)?\s*(?:role|field|position)",
]
 
# Phrases indicating no experience is required (mapped to 0 years)
_ZERO_EXPERIENCE_PATTERNS = [
    r"no\s*(?:prior\s*)?experience\s*(?:is\s*)?required",
    r"entry[\s-]?level",
    r"0\s*years?\s*(?:of\s*)?experience",
    r"new\s*grad(?:uate)?s?",
]

# education extraction
_EDUCATION_LEVELS = [
    ("PhD", [r"ph\.?d", r"doctorate", r"doctoral degree"]),
    ("Master's Degree", [r"master'?s degree", r"\bmba\b", r"\bm\.?s\.?\b(?=.*degree)", r"master'?s in"]),
    ("Bachelor's Degree", [r"bachelor'?s degree", r"\bb\.?s\.?\b(?=.*degree)", r"\bb\.?a\.?\b(?=.*degree)", r"bachelor'?s in", r"4[\s-]year degree"]),
    ("Associate's Degree", [r"associate'?s degree", r"associate'?s in", r"2[\s-]year degree"]),
    ("High School Diploma", [r"high school diploma", r"\bged\b", r"high school degree"]),
]


_REGEX_METACHARS = set(r"\(){}[]|.*+?^$")

def _wrap_boundary(pattern: str) -> str:
    """Match the boundary-wrapping behavior of skills_dictionary._compile_patterns
    so plain words don't match as substrings of other words."""
    if not any(c in pattern for c in _REGEX_METACHARS):
        return rf"\b{pattern}\b"
    return pattern

def add_skills_column(df, text_col: str = "job_description", output_col: str = "skills"):
    """Add an array<string> column of matched canonical skill names."""
    skill_exprs = []
    for skills in SKILLS_DICTIONARY.values():
        for canonical_name, patterns in skills.items():
            combined = "(?i)(" + "|".join(_wrap_boundary(p) for p in patterns) + ")"
            skill_exprs.append(F.when(F.col(text_col).rlike(combined), F.lit(canonical_name)))
 
    return df.withColumn(output_col, F.array_compact(F.array(*skill_exprs)))

def add_experience_years_column(df, text_col: str = "job_description", output_col: str = "extracted_min_years_experience"):
    """Add a float column with the minimum required years of experience."""
    candidate_exprs = []
 
    for pattern in list(_RANGE_PATTERNS) + list(_MIN_YEARS_PATTERNS):
        p = f"(?i){pattern}"
        extracted = F.regexp_extract(F.col(text_col), p, 1)
        candidate_exprs.append(F.when(extracted != "", extracted.cast("float")))
 
    min_years = F.array_min(F.array_compact(F.array(*candidate_exprs)))
 
    zero_pattern = "(?i)(" + "|".join(_ZERO_EXPERIENCE_PATTERNS) + ")"
    is_entry_level = F.col(text_col).rlike(zero_pattern)
 
    return df.withColumn(
        output_col,
        F.coalesce(min_years, F.when(is_entry_level, F.lit(0.0))),
    )
    
def add_education_level_column(df, text_col: str = "job_description", output_col: str = "extracted_education_level"):
    """Add a string column with the highest education level mentioned (PhD > ... > High School)."""
    matched_exprs = []
    for level_name, patterns in _EDUCATION_LEVELS:
        combined = "(?i)(" + "|".join(patterns) + ")"
        matched_exprs.append(F.when(F.col(text_col).rlike(combined), F.lit(level_name)))
 
    return df.withColumn(output_col, F.coalesce(*matched_exprs))