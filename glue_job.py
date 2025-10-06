import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

import boto3
from pyspark.sql.functions import col, avg, min, max, round
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# cliente do boto3 para Glue
glue_client = boto3.client("glue")

# variaveis
raw_path = "s3://dados-gabriel-espanhol/raw/" 
output_path = "s3://dados-gabriel-espanhol/refined/"
database_name = "workspaceDB"
table_name = "Nasdas100"

window = Window.partitionBy("Codigo_acao").orderBy("Data").rowsBetween(-29, 0)

# Lê parquet direto do S3
df = spark.read.option("recursiveFileLookup", "true").parquet(raw_path)

df.printSchema()


# filtra os dados entre as datas de inicio (30 dias atras) e a  data base (data da partição)
# df = df.filter((col("Date") >= start_dt) & (col("Date") <= dt))

# # renomeando colunas
df = df.withColumnRenamed("Date", "Data")  \
    .withColumnRenamed("Ticker", "Codigo_acao")  \
    .withColumnRenamed("Open", "Abertura_dia")  \
    .withColumnRenamed("Close", "Fechamento_dia")  \
    .withColumnRenamed("High", "Maximo_dia")  \
    .withColumnRenamed("Low", "Minimo_dia")
    
df.show(5)

# # Calculando valor medio 30d, valor maximo 30d, valor medio 30d, valor medio do dia
# # e
# # Arrdondando valores       
df = df.withColumn("media_movel_30d", round(avg("Fechamento_dia").over(window),2)) \
      .withColumn("max_30d", round(max("Maximo_dia").over(window),2)) \
      .withColumn("min_30d", round(min("Minimo_dia").over(window),2)) \
      .withColumn("Valor_medio_dia", round(((df.Maximo_dia + df.Minimo_dia)/2),2))

df = df.withColumn("Abertura_dia", round(df["Abertura_dia"],2)) \
      .withColumn("Maximo_dia", round(df["Maximo_dia"],2)) \
      .withColumn("Minimo_dia", round(df["Minimo_dia"],2)) \
      .withColumn("Fechamento_dia", round(df["Fechamento_dia"],2)) \

df.show(5)

df = df.withColumn("ano", col("ano").cast("int")) \
      .withColumn("mes", col("mes").cast("int")) \
      .withColumn("dia", col("dia").cast("int")) 
print("cast int")
df.show(5)


spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true") 

df.write \
  .mode("overwrite") \
  .partitionBy("ano", "mes", "dia", "Codigo_acao") \
  .option("compression", "snappy") \
  .parquet(output_path)
  
print("df.printSchema()")
df.printSchema()

# verifica se o banco de dados existe e crie se necessário
try:
    glue_client.get_database(Name=database_name)
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_database(
        DatabaseInput={'Name': database_name}
    )
    print(f"Banco de dados '{database_name}' criado no Glue Catalog.")

# definição da tabela transformada com partição
table_input = {
    'Name': 'Nasdas100',
    'StorageDescriptor': {
        'Columns': [
            {"Name": "Data", "Type": "timestamp"},
            {"Name": "Abertura_dia", "Type": "double"},
            {"Name": "Maximo_dia", "Type": "double"},
            {"Name": "Minimo_dia", "Type": "double"},
            {"Name": "Fechamento_dia", "Type": "double"},
            {"Name": "Volume", "Type": "bigint"},
            {"Name": "media_movel_30d", "Type": "double"},
            {"Name": "max_30d", "Type": "double"},
            {"Name": "min_30d", "Type": "double"},
            {"Name": "Valor_medio_dia", "Type": "double"}
        ],
        'Location': output_path,
        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        'Compressed': False,
        'SerdeInfo': {
            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'Parameters': {'serialization.format': '1'}
        }
    },
    'PartitionKeys': [
        {"Name": "ano", "Type": "int"},
        {"Name": "mes", "Type": "int"},
        {"Name": "dia", "Type": "int"},
        {"Name": "Codigo_acao", "Type": "string"}
    ],
    'TableType': 'EXTERNAL_TABLE',
    'Parameters': {
        'classification': 'parquet',
        'EXTERNAL': 'TRUE'
    }
}

# cria ou atualiza a tabela transformada no Glue Catalog
try:
    glue_client.get_table(DatabaseName=database_name, Name=table_name)
    print(f"Tabela '{table_name}' já existe no Glue Catalog. Atualizando tabela...")
    glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
    print(f"Tabela '{table_name}' atualizada no Glue Catalog.")
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
    print(f"Tabela '{table_name}' criada no Glue Catalog.")

print(f"Tabela '{table_name}' disponível no Athena.")

# executa MSCK REPAIR TABLE para descobrir partições
repair_table_query = f"MSCK REPAIR TABLE {database_name}.{table_name}"
print(f"Executando comando: {repair_table_query}")
spark.sql(repair_table_query)
print(f"Comando MSCK REPAIR TABLE executado com sucesso para a tabela '{table_name}'.")



job.commit()
