import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def check_existencia_local(path, title):
    """
    Verifica se um arquivo existe em um diretório local.

    :param path: str - O caminho do diretório local (ex: 'data/raw/').
    :param title: str - O nome do arquivo a ser verificado, com a extensão (ex: 'dados.csv').

    :return: bool - True se o arquivo existe, False caso contrário.
    """
    try:
        caminho_completo = Path(path) / title
        print(caminho_completo)
        if caminho_completo.is_file():
            print(f"O arquivo '{caminho_completo}' já existe localmente.")
            return True
        else:
            print(f"O arquivo '{caminho_completo}' ainda não existe localmente.")
            return False
            
    except Exception as e:
        print(f"Erro ao acessar o caminho '{Path(path) / title}': {e}")
        return False


def check_existencia_s3(bucket_name, path, title, region='us-east-1'):
    """
    Verifica se um objeto existe em um bucket S3.

    :param bucket_name: str - O nome do bucket S3 de origem.
    :param path: str - O caminho do diretório dentro do bucket (ex: 'data/raw/').
    :param title: str - O nome do arquivo CSV a ser lido, com a extensão .csv (ex: 'dados.csv').
    :param region: str - A região da AWS onde o bucket S3 está localizado.

    :return: pandas.DataFrame - O DataFrame carregado do arquivo CSV.
    :raises FileNotFoundError: Se o objeto não for encontrado no S3.
    """

    s3_client = boto3.client('s3', region_name=region)
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"{path}/{title}"
        )
        if 'Contents' in response and len(response['Contents']) > 0:
            print(f"O arquivo s3://{bucket_name}/{path}/{title} já existe.")
            return True
        else:
            print(f"O arquivo s3://{bucket_name}/{path}/{title} ainda não existe.")
            return False
            
    except Exception as e:
        print(f"Erro ao acessar o caminho s3://{bucket_name}/{path}/{title}: {e}")
        return False
    

def check_existencia(local, path, title,  extensao = '', region=None, bucket_name = None):
    """
    Verifica se um objeto existe localmente ou no Bucket S3.

    :param path: str - O caminho do diretório dentro do bucket (ex: 'data/raw/').
    :param title: str - O nome do arquivo CSV a ser lido, sem a extensão .csv (ex: 'dados').
    :param title: str - Extensao do arquivo (ex: .csv)
    :param region: str - Caso local = s3, a região da AWS onde o bucket S3 está localizado.
    :param bucket_name: str - Caso local = s3, o nome do bucket S3 de origem.

    :return: pandas.DataFrame - O DataFrame carregado do arquivo CSV.
    :raises FileNotFoundError: Se o objeto não for encontrado no S3.
    """
    if local in ('s3', 'S3'):
        return check_existencia_s3(bucket_name, path, title, region)
        
    else:
        path = f'{local}/{path}'
        title = f'{title}{extensao}'
        return check_existencia_local(path, title)
    