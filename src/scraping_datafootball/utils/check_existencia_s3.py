import boto3
from botocore.exceptions import ClientError
from datetime import datetime

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