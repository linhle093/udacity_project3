import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer landing
customerlanding_node1718551198706 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://linhltt-project3-udacity/customer/landing/"], "recurse": True}, transformation_ctx="customerlanding_node1718551198706")

# Script generated for node landing to trusted rule
landingtotrustedrule_node1718551243841 = Filter.apply(frame=customerlanding_node1718551198706, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="landingtotrustedrule_node1718551243841")

# Script generated for node customer trusted
customertrusted_node1718551248739 = glueContext.write_dynamic_frame.from_options(frame=landingtotrustedrule_node1718551243841, connection_type="s3", format="json", connection_options={"path": "s3://linhltt-project3-udacity/customer/trusted/", "partitionKeys": []}, transformation_ctx="customertrusted_node1718551248739")

job.commit()