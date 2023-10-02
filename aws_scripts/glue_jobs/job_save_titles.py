# Save titles content details.

import sys
import pyspark.sql.functions as F

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import DoubleType

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
df = df.withColumn("object_type", F.col("objectType"))
df = df.withColumn("title", F.col("content_title"))
df = df.withColumn("short_description", F.col("content_shortDescription"))
df = df.withColumn("release_year", F.col("content_originalReleaseYear"))
df = df.withColumn("age_dertification", F.col("content_ageCertification"))
df = df.withColumn("runtime", F.col("content_runtime"))
df = df.withColumn("full_path", F.col("content_fullPath"))
df = df.withColumn("imdb_score", F.col("content_scoring_imdbScore").cast(DoubleType()))
df = df.withColumn("imdb_votes", F.col("content_scoring_imdbVotes"))
df = df.withColumn("tmdb_popolarity", F.col( "content_scoring_tmdbPopularity"))
df = df.withColumn("tmbd_score", F.col("content_scoring_tmdbScore"))
df = df.withColumn("season_count", F.col("totalSeasonCount"))

df = df.na.fill(0)

df = df.select("title_id", "object_type", "title", "short_description", "release_year", 
               "age_dertification", "runtime", "full_path", "imdb_score", "imdb_votes", 
               "tmdb_popolarity", "tmbd_score", "season_count")
df = df.dropDuplicates()

df.show()
df.write.parquet("s3://justwatch-bucket/data/processed/titles/", mode="overwrite")

job.commit()