import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accelerometer trusted
accelerometertrusted_node1718586228542 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://linhltt-project3-udacity/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometertrusted_node1718586228542")

# Script generated for node step trainer trusted
steptrainertrusted_node1718586229208 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://linhltt-project3-udacity/step_trainer/trusted/"], "recurse": True}, transformation_ctx="steptrainertrusted_node1718586229208")

# Script generated for node Join
Join_node1718586244041 = Join.apply(frame1=steptrainertrusted_node1718586229208, frame2=accelerometertrusted_node1718586228542, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1718586244041")

# Script generated for node Drop Fields
DropFields_node1718586254346 = DropFields.apply(frame=Join_node1718586244041, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="DropFields_node1718586254346")

# Script generated for node Drop Duplicates
DropDuplicates_node1718586257540 =  DynamicFrame.fromDF(DropFields_node1718586254346.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1718586257540")

# Script generated for node machine learning curated
machinelearningcurated_node1718586312407 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1718586257540, connection_type="s3", format="json", connection_options={"path": "s3://linhltt-project3-udacity/step_trainer/curated/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="machinelearningcurated_node1718586312407")

job.commit()