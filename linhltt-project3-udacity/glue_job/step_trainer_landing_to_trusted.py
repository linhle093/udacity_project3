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

# Script generated for node step trainer landing
steptrainerlanding_node1718582044371 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://linhltt-project3-udacity/step_trainer/landing/"], "recurse": True}, transformation_ctx="steptrainerlanding_node1718582044371")

# Script generated for node customer curated
customercurated_node1718582043611 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://linhltt-project3-udacity/customer/curated/"], "recurse": True}, transformation_ctx="customercurated_node1718582043611")

# Script generated for node Join
Join_node1718582060924 = Join.apply(frame1=steptrainerlanding_node1718582044371, frame2=customercurated_node1718582043611, keys1=["serialnumber"], keys2=["serialNumber"], transformation_ctx="Join_node1718582060924")

# Script generated for node Drop Fields
DropFields_node1718582105199 = DropFields.apply(frame=Join_node1718582060924, paths=["email", "phone", "serialNumber", "shareWithPublicAsOfDate", "birthDay", "registrationDate", "shareWithResearchAsOfDate", "customerName", "shareWithFriendsAsOfDate", "lastUpdateDate"], transformation_ctx="DropFields_node1718582105199")

# Script generated for node Drop Duplicates
DropDuplicates_node1718582109171 =  DynamicFrame.fromDF(DropFields_node1718582105199.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1718582109171")

# Script generated for node step trainer trusted
steptrainertrusted_node1718582064005 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1718582109171, connection_type="s3", format="json", connection_options={"path": "s3://linhltt-project3-udacity/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="steptrainertrusted_node1718582064005")

job.commit()