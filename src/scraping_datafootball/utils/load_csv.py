import json
import boto3
import os
import pandas as pd
import io
from dotenv import load_dotenv

# Uso das credenciais para teste
# load_dotenv()

# aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
# aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
# aws_region = os.environ.get('AWS_DEFAULT_REGION')

# s3_client = boto3.client(
#     's3',
#     region_name=aws_region # Opcional: Se a variável de região estiver definida
# )

def load_csv_from_s3(bucket_name, path, title, region='us-east-1', sep=','):
    """
    Carrega e retorna um arquivo CSV de um bucket do Amazon S3 como um DataFrame do pandas.

    :param bucket_name: str - O nome do bucket S3 de origem.
    :param path: str - O caminho do diretório dentro do bucket (ex: 'data/raw/').
    :param title: str - O nome do arquivo CSV a ser lido, com a extensão .csv (ex: 'dados.csv').
    :param region: str - A região da AWS onde o bucket S3 está localizado.
    :param sep: str - O caractere delimitador do arquivo CSV (padrão: ',').

    :return: pandas.DataFrame - O DataFrame carregado do arquivo CSV.
    :raises FileNotFoundError: Se o objeto não for encontrado no S3.
    """
    try:
        s3 = boto3.client('s3', region_name=region)
        obj = s3.get_object(
            Bucket=bucket_name,
            Key=f"{path}/{title}.csv"
        )
        
        # Lê o conteúdo do arquivo em bytes
        csv_data = obj['Body'].read()

        # Usa io.BytesIO para ler o conteúdo como um arquivo em memória
        # ➡️ Correção: Adicionado o parâmetro sep
        df = pd.read_csv(io.BytesIO(csv_data), sep=sep)
        
        return df

    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(f"Objeto não encontrado no S3: s3://{bucket_name}/{path}/{title}")
    except Exception as e:
        raise Exception(f"Erro ao carregar o arquivo CSV do S3: {e}")