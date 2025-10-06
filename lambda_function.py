import json
import boto3

glue = boto3.client("glue")

def lambda_handler(event, context):
  
# dispara job do Glue
    response = glue.start_job_run(
        JobName="job_transform",
    )

    print(f"Glue Job iniciado: {response['JobRunId']}")

    return {"statusCode": 200}
