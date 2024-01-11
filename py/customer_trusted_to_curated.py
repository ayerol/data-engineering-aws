import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


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
SqlQuery418 = """
SELECT
    customer.*
FROM
    accelerometer
INNER JOIN
    customer ON accelerometer.user = customer.email;
"""
join_node1704876804897 = sparkSqlQuery(
    glueContext,
    query=SqlQuery418,
    mapping={
        "customer": CustomerTrusted_node1704876343209,
        "accelerometer": AccelerometerLanding_node1704876406982,
    },
    transformation_ctx="join_node1704876804897",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1704969641610 = DynamicFrame.fromDF(
    join_node1704876804897.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1704969641610",
)

# Script generated for node Customer Curated
CustomerCurated_node1704875467832 = glueContext.getSink(
    path="s3://stedi-lake-bucket-udacity/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1704875467832",
)
CustomerCurated_node1704875467832.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node1704875467832.setFormat("json")
CustomerCurated_node1704875467832.writeFrame(DropDuplicates_node1704969641610)
job.commit()
