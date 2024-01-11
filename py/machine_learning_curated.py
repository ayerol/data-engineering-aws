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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1704971747887 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1704971747887",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1704975268287 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1704975268287",
)

# Script generated for node SQL Query
SqlQuery417 = """
SELECT
    accelerometer.*,
    trainer.serialNumber,
    trainer.distanceFromObject
FROM
    accelerometer
INNER JOIN
    trainer ON accelerometer.timeStamp = trainer.sensorReadingTime;
"""
SQLQuery_node1704971803504 = sparkSqlQuery(
    glueContext,
    query=SqlQuery417,
    mapping={
        "accelerometer": AccelerometerTrusted_node1704971747887,
        "trainer": StepTrainerTrusted_node1704975268287,
    },
    transformation_ctx="SQLQuery_node1704971803504",
)

# Script generated for node Machine Learning
MachineLearning_node1704972172158 = glueContext.getSink(
    path="s3://stedi-lake-bucket-udacity/machine_learning/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearning_node1704972172158",
)
MachineLearning_node1704972172158.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearning_node1704972172158.setFormat("json")
MachineLearning_node1704972172158.writeFrame(SQLQuery_node1704971803504)
job.commit()
