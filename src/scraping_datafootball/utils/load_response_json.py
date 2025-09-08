import json
import boto3
from dotenv import load_dotenv
import os

def load_response_json_local(file_path):
    file_path = os.path.join(file_path)
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_response_json_s3(bucket_name, path, title, region='us-east-1'):
    """
    Carrega e retorna um objeto JSON de um bucket do Amazon S3.

    :param bucket_name: str - O nome do bucket S3 de origem.
    :param path: str - O caminho do diretório dentro do bucket (ex: 'data/raw/').
    :param title: str - O nome do arquivo a ser lido, sem a extensão (ex: 'sports').
    :param region: str - A região da AWS onde o bucket S3 está localizado.

    :return: dict - O objeto JSON (dicionário Python) carregado do S3.
    :raises FileNotFoundError: Se o objeto não for encontrado no S3.
    """
    try:
        s3 = boto3.client('s3', region_name=region)
        obj = s3.get_object(
            Bucket=bucket_name, 
            Key=f"{path}/{title}"
            )
        response_data = json.load(obj['Body'])
        
        return response_data
    
    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(f"Objeto não encontrado no S3: s3://{bucket_name}/{path}/{title}")
    except Exception as e:
        raise Exception(f"Erro ao carregar o arquivo JSON do S3: {e}")
    

def load_response_json(local, path, title, region=None, bucket_name = None):
    """
    Carrega e retorna um objeto JSON de uma pasta local ou de um bucket do Amazon S3.

    :param path: str - O caminho do diretório dentro do bucket (ex: 'data/raw/').
    :param title: str - O nome do arquivo CSV a ser lido, com a extensão .csv (ex: 'dados.csv').
    :param region: str - Caso local = s3, a região da AWS onde o bucket S3 está localizado.
    :param bucket_name: str - Caso local = s3, o nome do bucket S3 de origem.

    :return: dict - O objeto JSON (dicionário Python) carregado.
    :raises FileNotFoundError: Se o objeto não for encontrado no S3.
    """

    if local in ('s3', 'S3'):
        return load_response_json_s3(bucket_name, path, title, region)
            
    else:
        path = f'{local}/{path}'
        return load_response_json_local(path)
    