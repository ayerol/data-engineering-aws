import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1704876343209 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1704876343209",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1704876406982 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1704876406982",
)

# Script generated for node join
SqlQuery154 = """
SELECT
    accelerometer.*
FROM
    customer
INNER JOIN
    accelerometer ON customer.email = accelerometer.user;

"""
join_node1704876804897 = sparkSqlQuery(
    glueContext,
    query=SqlQuery154,
    mapping={
        "customer": CustomerTrusted_node1704876343209,
        "accelerometer": AccelerometerLanding_node1704876406982,
    },
    transformation_ctx="join_node1704876804897",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1704875467832 = glueContext.write_dynamic_frame.from_options(
    frame=join_node1704876804897,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-bucket-udacity/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1704875467832",
)

job.commit()
