# Save streamings data, which is called 'package' in the Justwatch context.

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
df = df.withColumn("streaming_service_id", F.col("offers.package.packageId"))
df = df.withColumn("clear_name", F.col("offers.package.clearName"))
df = df.withColumn("slug", F.col("offers.package.slug"))
df = df.withColumn("icon", F.col("offers.package.icon"))
df = df.select("streaming_service_id", "clear_name", "slug", "icon")
df = df.dropDuplicates()

df.show()
df.write.parquet("s3://justwatch-bucket/data/processed/streaming_services/", mode="overwrite")

job.commit()