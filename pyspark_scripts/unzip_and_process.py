import requests
import boto3
#import os

from io import BytesIO
from pyspark.sql import SparkSession
from zipfile import ZipFile, BadZipFile

def upload_zip_to_s3(url, s3_bucket, s3_key):
    # Baixar o arquivo zip do link
    response = requests.get(url)

    # Verificar se a solicitação foi bem-sucedida
    if response.status_code == 200:

        # Converter o conteúdo do arquivo para bytes
        zip_bytes = BytesIO(response.content)

        # Inicializar o cliente S3
        s3_client = boto3.client('s3')

        try:
            # Caminho completo no S3 incluindo a pasta "RAW" e o nome do arquivo ZIP
            s3_path = f"RAW/{s3_key}"

            # Upload do arquivo para o Amazon S3
            s3_client.upload_fileobj(zip_bytes, s3_bucket, s3_path)

            print(f"Arquivo ZIP enviado com sucesso para o S3 em: s3a://{s3_bucket}/{s3_path}")
        except Exception as e:
            print(f"Erro ao enviar o arquivo para o S3: {e}")
        finally:
            # Fechar o cliente S3
            s3_client.close()
    else:
        print(f"Erro ao baixar o arquivo. Código de status: {response.status_code}")

def processar_zip_no_s3(s3_bucket, zip_file_key):
    # Criar um cliente S3
    s3_client = boto3.client('s3')

    try:
        # Baixar o conteúdo do arquivo ZIP do S3
        response = s3_client.get_object(Bucket=s3_bucket, Key=zip_file_key)
        zip_data = response['Body'].read()

        # Extrair o conteúdo do arquivo ZIP em memória
        with ZipFile(BytesIO(zip_data)) as zip_ref:
            # Lista de nomes de arquivos extraídos
            file_list = zip_ref.namelist()

            # Extrair cada arquivo e salvar no S3
            for file_name in file_list:
                with zip_ref.open(file_name) as file:
                    s3_target_key = f"RAW/{file_name}"
                    s3_client.put_object(Bucket=s3_bucket, Key=s3_target_key, Body=file.read())

        print("Deszipado com sucesso.")

    except BadZipFile as e:
        print(f"Erro ao deszipar: {str(e)}")
    except Exception as e:
        print(f"Erro inesperado: {str(e)}")

def creating_archive_folder(S3_BUCKET, zip_file_key, output_path):
    try:
        s3 = boto3.client('s3')

        # Copia o objeto para o novo destino
        s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': zip_file_key}, Key=output_path)

        # Exclui o objeto original
        s3.delete_object(Bucket=S3_BUCKET, Key=zip_file_key)

        print("Arquivo copiado com Sucesso")

    except Exception as e:
        print(f"Deu merda familia: {str(e)}")

def csv_to_parquet(input_csv_path, output_parquet_path):

    spark = SparkSession.builder.appName("Application").getOrCreate()

    #spark.sparkContext._jsc.hadoopConfiguration().set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.1.2,com.amazonaws:aws-java-sdk-bundle:1.11.271')
    #spark.sparkContext._jsc.hadoopConfiguration().set('spark.hadoop.fs.s3a.access.key', os.environ.get('AWS_ACCESS_KEY_ID'))
    #spark.sparkContext._jsc.hadoopConfiguration().set('spark.hadoop.fs.s3a.secret.key', os.environ.get('AWS_SECRET_ACCESS_KEY'))
    #spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    #spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.path.style.access", "true")
    #spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    #spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.connection.maximum", "200")
    
    try:
        # Lê o CSV e realiza as transformações necessárias
        df = spark.read.format("csv").option("header", "true").load(input_csv_path)
        df = df \
            .withColumnRenamed("Organization Id", "Organization_Id") \
            .withColumnRenamed("Number of employees", "Number_of_employees")
        
        # Reparticiona o DataFrame para ter apenas 1 partição
        df = df.repartition(1)

        # Escreve o DataFrame em formato Parquet na pasta de saída
        df.write.parquet(output_parquet_path, mode="overwrite")

        print(f"CSV transformado em Parquet e salvo em: {output_parquet_path}")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")

def create_glue_database(database_name, region_name):
    glue_client = boto3.client('glue', region_name=region_name)
    
    try:
        response = glue_client.create_database(
            DatabaseInput={'Name': database_name}
        )
        print(f'Banco de dados "{database_name}" criado com sucesso no AWS Glue Data Catalog.')
    except Exception:
        print(f'A database "{database_name}" já existe.')

def create_glue_crawler(crawler_name, database_name, s3_target, region_name):
    glue_client = boto3.client('glue', region_name=region_name)
    
    try:
        response = glue_client.create_crawler(
            Name=crawler_name,
            Role='arn:aws:iam::198763845825:role/glue-crawler-role', 
            DatabaseName=database_name,
            Targets={'S3Targets': [{'Path': s3_target}]}
        )
        print(f'Crawler "{crawler_name}" criado com sucesso no AWS Glue.')
    except glue_client.exceptions.AlreadyExistsException:
        print(f'O crawler "{crawler_name}" já existe.')
    except Exception as e:
        print(f'Erro ao criar o crawler: {str(e)}')


if __name__ == '__main__':

    S3_BUCKET = 'bucket-organizations'
    CRAWLER_NAME = 'organizations_crawler'
    S3_TARGET = 's3://bucket-organizations/PROCESSING/'
    DATABASE_NAME = 'organizations-data'
    REGION_NAME = 'us-east-1'

    url = "https://github.com/datablist/sample-csv-files/raw/main/files/organizations/organizations-2000000.zip"
    s3_key = "organizations.zip"

    upload_zip_to_s3(url, S3_BUCKET, s3_key)

    zip_file_key = "RAW/organizations.zip"

    processar_zip_no_s3(S3_BUCKET, zip_file_key)
    
    output_path = "ARCHIVES/organizations.zip"

    creating_archive_folder(S3_BUCKET, zip_file_key, output_path)

    csv_path = f"s3://{S3_BUCKET}/RAW/organizations-2000000.csv"
    parquet_output_path = f"s3://{S3_BUCKET}/PROCESSING/"

    csv_to_parquet(csv_path, parquet_output_path)

    create_glue_database(DATABASE_NAME, REGION_NAME)

    create_glue_crawler(CRAWLER_NAME, DATABASE_NAME, S3_TARGET, REGION_NAME)


# spark-submit --conf "spark.jars.packages=org.apache.hadoop:hadoop-aws:3.1.2,com.amazonaws:aws-java-sdk-bundle:1.11.271" unzip_and_process.py > t.log