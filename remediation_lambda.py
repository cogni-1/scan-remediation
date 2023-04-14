import json 
import boto3
from urllib.parse import unquote
def lambda_handler(event, context):
   glue = boto3.client("glue")
   file_name = unquote(event['Records'][0]['s3']['object']['key'])
   bucket_name = unquote(event['Records'][0]['s3']['bucket']['name'])
   print("File Name : ", file_name)
   print("Bucket Name : ",bucket_name)
   print("Glue Job name : Remediation Demo Notebook")
   response = glue.start_job_run(JobName = "Remediation Demo Notebook",   Arguments = {"--file":file_name,"--bucket":bucket_name})
   print("Lambda invoke")
