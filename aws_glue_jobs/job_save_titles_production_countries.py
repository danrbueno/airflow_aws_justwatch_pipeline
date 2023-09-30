# Save titles and countries associated table.

import sys
import pyspark.sql.functions as F

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dyf = glueContext.create_dynamic_frame.from_catalog(
    database="justwatch", table_name="staging"
)

df = dyf.toDF()
df = df.withColumn("title_id", F.col("id"))
df = df.withColumn("country_code", F.explode(F.col("content_productionCountries")))
df = df.select("title_id","country_code")
df = df.dropDuplicates()

df.show()
df.write.parquet("s3://justwatch-bucket/data/processed/titles_production_countries/", mode="overwrite")

job.commit()