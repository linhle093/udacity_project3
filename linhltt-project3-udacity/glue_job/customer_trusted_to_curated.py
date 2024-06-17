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

# Script generated for node accelerometer landing
accelerometerlanding_node1718559056664 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://linhltt-project3-udacity/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometerlanding_node1718559056664")

# Script generated for node customer trusted
customertrusted_node1718559056187 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://linhltt-project3-udacity/customer/trusted/"], "recurse": True}, transformation_ctx="customertrusted_node1718559056187")

# Script generated for node Join
Join_node1718559060703 = Join.apply(frame1=accelerometerlanding_node1718559056664, frame2=customertrusted_node1718559056187, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1718559060703")

# Script generated for node Drop Fields
DropFields_node1718559287475 = DropFields.apply(frame=Join_node1718559060703, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="DropFields_node1718559287475")

# Script generated for node Drop Duplicates
DropDuplicates_node1718560111070 =  DynamicFrame.fromDF(DropFields_node1718559287475.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1718560111070")

# Script generated for node customer curated
customercurated_node1718559063255 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1718560111070, connection_type="s3", format="json", connection_options={"path": "s3://linhltt-project3-udacity/customer/curated/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="customercurated_node1718559063255")

job.commit()