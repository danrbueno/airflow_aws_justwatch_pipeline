# Save offers data.

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
df = df.withColumn("offers", F.explode(F.col("offers")))
df = df.withColumn("title_streaming_service_id", F.col("offers.id"))
df = df.withColumn("standard_web_url", F.col("offers.standardWebURL"))
df = df.withColumn("title_id", F.col("id"))
df = df.withColumn("streaming_service_id", F.col("offers.package.packageId"))

df = df.select("title_streaming_service_id", "standard_web_url", "title_id", "streaming_service_id")
df = df.dropDuplicates()

df.show()
df.write.parquet("s3://justwatch-bucket/data/processed/titles_streaming_services/", mode="overwrite")

job.commit()