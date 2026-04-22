import json
import boto3
glueClient = boto3.client('glue')
def lambda_handler(event, context):
	glueClient.start_job_run(JobName="jobname")  # Replace the jobname with actual job name , which is created
	return "Job started"
