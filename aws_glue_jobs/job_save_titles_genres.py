# Save titles and genres associated table.

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
df = df.withColumn("genres", F.explode(F.col("content_genres")))
df = df.withColumn("genre_id", F.col("genres.id"))
df = df.select("title_id", "genre_id")
df = df.dropDuplicates()

df.show()
df.write.parquet("s3://justwatch-bucket/data/processed/titles_genres/", mode="overwrite")

job.commit()