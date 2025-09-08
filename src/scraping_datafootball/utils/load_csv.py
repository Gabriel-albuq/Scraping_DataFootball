import json
import boto3
import os
import pandas as pd
import io
from dotenv import load_dotenv
from pathlib import Path

def load_csv_local(path, title, sep=','):
    """
    Carrega e retorna um arquivo CSV do sistema de arquivos local como um DataFrame do pandas.

    :param path: str - O caminho do diretório local (ex: 'data/raw/').
    :param title: str - O nome do arquivo CSV a ser lido, com a extensão .csv (ex: 'dados.csv').
    :param sep: str - O caractere delimitador do arquivo CSV (padrão: ',').

    :return: pandas.DataFrame - O DataFrame carregado do arquivo CSV.
    :raises FileNotFoundError: Se o arquivo não for encontrado no caminho especificado.
    """
    try:
        caminho_completo = Path(path) / title
        
        df = pd.read_csv(caminho_completo, sep=sep)
        
        print(f"Arquivo '{caminho_completo}' carregado com sucesso.")
        return df

    except FileNotFoundError:
        caminho_completo = Path(path) / title
        raise FileNotFoundError(f"Arquivo não encontrado no caminho local: {caminho_completo}")
    except Exception as e:
        caminho_completo = Path(path) / title
        raise Exception(f"Erro ao carregar o arquivo CSV '{caminho_completo}': {e}")


def load_csv_s3(bucket_name, path, title, region='us-east-1', sep=','):
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
        df = pd.read_csv(io.BytesIO(csv_data), sep=sep)
        
        return df

    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(f"Objeto não encontrado no S3: s3://{bucket_name}/{path}/{title}")
    except Exception as e:
        raise Exception(f"Erro ao carregar o arquivo CSV do S3: {e}")
    

def load_csv(local, path, title, region=None, bucket_name = None):
    """
    Verifica se um objeto existe localmente ou no Bucket S3.

    :param path: str - O caminho do diretório dentro do bucket (ex: 'data/raw/').
    :param title: str - O nome do arquivo CSV a ser lido, com a extensão .csv (ex: 'dados.csv').
    :param region: str - Caso local = s3, a região da AWS onde o bucket S3 está localizado.
    :param bucket_name: str - Caso local = s3, o nome do bucket S3 de origem.

    :return: pandas.DataFrame - O DataFrame carregado do arquivo CSV.
    :raises FileNotFoundError: Se o objeto não for encontrado no S3.
    """

    if local in ('s3', 'S3'):
        return load_csv_s3(bucket_name, path, title, region, sep=',')
        
    else:
        path = f'{local}/{path}'
        return load_csv_local(path, title, sep=',')
    