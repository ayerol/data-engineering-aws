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

# Script generated for node Customer Curated
CustomerCurated_node1704971162842 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1704971162842",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1704971177043 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1704971177043",
)

# Script generated for node SQL Query
SqlQuery416 = """
SELECT
    trainer.*
FROM
    customer
INNER JOIN
    trainer ON customer.serialNumber = trainer.serialNumber;
"""
SQLQuery_node1704971204574 = sparkSqlQuery(
    glueContext,
    query=SqlQuery416,
    mapping={
        "trainer": StepTrainerLanding_node1704971177043,
        "customer": CustomerCurated_node1704971162842,
    },
    transformation_ctx="SQLQuery_node1704971204574",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1704974834422 = glueContext.getSink(
    path="s3://stedi-lake-bucket-udacity/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1704974834422",
)
StepTrainerTrusted_node1704974834422.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1704974834422.setFormat("json")
StepTrainerTrusted_node1704974834422.writeFrame(SQLQuery_node1704971204574)
job.commit()
