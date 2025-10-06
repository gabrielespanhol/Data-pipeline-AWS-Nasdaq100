import json
import boto3

# cria client fora do handler (boa prática para reuso de conexão)
glue = boto3.client("glue")

def lambda_handler(event, context):
  
# dispara job do Glue com parâmetros
    response = glue.start_job_run(
        JobName="job_transform",
    )

    print(f"Glue Job iniciado: {response['JobRunId']}")

    return {"statusCode": 200}
