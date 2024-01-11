import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1704806188177 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-bucket-udacity/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1704806188177",
)

# Script generated for node Share with Research
SharewithResearch_node1704806235220 = Filter.apply(
    frame=CustomerLanding_node1704806188177,
    f=lambda row: (not (row["sharewithresearchasofdate"] == 0)),
    transformation_ctx="SharewithResearch_node1704806235220",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1704806375850 = glueContext.write_dynamic_frame.from_options(
    frame=SharewithResearch_node1704806235220,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-bucket-udacity/customer/trusted/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node1704806375850",
)

job.commit()
