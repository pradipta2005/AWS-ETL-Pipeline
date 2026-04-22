import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_null_rows
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1776880074330 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://messy-data-input"], "recurse": True}, transformation_ctx="AmazonS3_node1776880074330")

# Script generated for node Drop Duplicates
DropDuplicates_node1776880122471 =  DynamicFrame.fromDF(AmazonS3_node1776880074330.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1776880122471")

# Script generated for node Remove Null Rows
RemoveNullRows_node1776880299892 = DropDuplicates_node1776880122471.gs_null_rows(extended=False)

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RemoveNullRows_node1776880299892, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776877963205", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1776880546580 = glueContext.write_dynamic_frame.from_options(frame=RemoveNullRows_node1776880299892, connection_type="s3", format="csv", connection_options={"path": "s3://data-output-clean", "partitionKeys": []}, transformation_ctx="AmazonS3_node1776880546580")

job.commit()
