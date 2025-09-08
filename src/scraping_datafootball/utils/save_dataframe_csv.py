import os
import json
import boto3
import io
from dotenv import load_dotenv

def save_dataframe_csv(df, path, title):
    """
    Salva um DataFrame como CSV no caminho especificado com o título dado.

    :param df: pd.DataFrame - O DataFrame a ser salvo.
    :param path: str - O caminho do diretório onde o arquivo será salvo.
    :param title: str - O título (nome) do arquivo CSV.
    """
    os.makedirs(path, exist_ok=True)
    file_path = os.path.join(path, f"{title}.csv")
    
    df.to_csv(file_path, index=False, encoding='utf-8')
    print(f"Arquivo salvo com sucesso em: {file_path}")

def save_dataframe_csv_s3(df, bucket_name, path, title, region='us-east-1'):
    """
    Salva um DataFrame como arquivo CSV em um bucket do Amazon S3.

    :param df: pd.DataFrame - O DataFrame a ser salvo.
    :param bucket_name: str - O nome do bucket S3 de destino.
    :param path: str - O caminho do diretório dentro do bucket (ex: 'data/raw/').
    :param title: str - O nome do arquivo a ser salvo, sem a extensão (ex: 'esportes').
    :param region: str - A região da AWS onde o bucket S3 está localizado.

    :return: str - O caminho completo (key) do arquivo salvo no S3 (ex: 'data/raw/esportes.csv').
    """
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8')
    csv_buffer.seek(0)
    
    file_key = f"{path}/{title}.csv"
    
    try:
        # Criação do cliente S3 (apenas uma vez)
        s3 = boto3.client('s3', region_name=region)
        
        s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_buffer.getvalue(),  # Passar o valor da string do buffer
            ContentType='text/csv'       # Tipo de conteúdo correto
        )
        
        print(f"Arquivo CSV salvo com sucesso no S3: s3://{bucket_name}/{file_key}")
        return file_key # Retornar o caminho completo para uso futuro
    
    except Exception as e:
        raise Exception(f"Erro ao salvar o DataFrame como CSV no S3: {e}")
    
def save_dataframe_csv(df, local, path, title, region=None, bucket_name = None):
    """
    Verifica se um objeto existe localmente ou no Bucket S3.

    :param df: pd.DataFrame - O DataFrame a ser salvo.
    :param path: str - O caminho do diretório dentro do bucket (ex: 'data/raw/').
    :param title: str - O nome do arquivo CSV a ser lido, com a extensão .csv (ex: 'dados.csv').
    :param region: str - Caso local = s3, a região da AWS onde o bucket S3 está localizado.
    :param bucket_name: str - Caso local = s3, o nome do bucket S3 de origem.

    :return: str - O caminho completo local ou a key do arquivo salvo no S3 (ex: 'data/raw/esportes.csv').
    """

    if local in ('s3', 'S3'):
        return save_dataframe_csv_s3(df, bucket_name, path, title, region)
        
    else:
        path = f'{local}/{path}'
        return save_dataframe_csv(df, path, title)