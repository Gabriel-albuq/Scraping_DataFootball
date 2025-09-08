import os
import json
import boto3
from io import StringIO
from dotenv import load_dotenv

def save_response_json_local(data, path, title):
    """
    Salva dados em formato JSON no caminho especificado com o título dado.

    :param data: dict ou qualquer objeto serializável - Os dados a serem salvos no formato JSON.
    :param path: str - O caminho do diretório onde o arquivo será salvo.
    :param title: str - O título (nome) do arquivo JSON.
    """
    # Garante que o caminho exista
    os.makedirs(path, exist_ok=True)
    
    # Concatena o caminho com o título e a extensão .json
    file_path = os.path.join(path, f"{title}.json")
    
    # Salva os dados como JSON
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    print(f"Arquivo JSON salvo com sucesso em: {file_path}")

    return True


def save_response_json_s3(data, bucket_name, path, title, region='us-east-1'):
    """
    Salva dados em formato JSON diretamente em um bucket do Amazon S3.

    :param data: dict - Os dados a serem serializados e salvos.
    :param bucket_name: str - O nome do bucket S3 de destino.
    :param path: str - O caminho do diretório dentro do bucket (ex: 'data/raw/').
    :param title: str - O nome do arquivo a ser salvo, sem a extensão (ex: 'esportes').
    :param region: str - A região da AWS onde o bucket S3 está localizado.

    :return: str - True se tudo der certo ou o erro.
    """
    try:
        json_data = json.dumps(data, ensure_ascii=False, indent=4)

        s3 = boto3.client('s3', region_name=region)
        s3.put_object(
            Bucket=bucket_name,
            Key=f"{path}/{title}",
            Body=json_data,
            ContentType='application/json'
        )
        
        print(f"Arquivo JSON salvo com sucesso no s3://{bucket_name}/{path}/{title}")
        return True

    except Exception as e:
        print(f"Erro ao salvar o arquivo JSON no S3: {e}")
        raise e
    
    
def save_response_json(data, local, path, title, region=None, bucket_name = None):
    """
    Salva dados em formato JSON localmente ou em um bucket do Amazon S3.

    :param data: dict - Os dados a serem serializados e salvos.
    :param path: str - O caminho do diretório dentro do bucket (ex: 'data/raw/').
    :param title: str - O nome do arquivo CSV a ser lido, com a extensão .csv (ex: 'dados.csv').
    :param region: str - Caso local = s3, a região da AWS onde o bucket S3 está localizado.
    :param bucket_name: str - Caso local = s3, o nome do bucket S3 de origem.

    :return: str - True se tudo der certo ou o erro.
    """

    if local in ('s3', 'S3'):
        return save_response_json_s3(data, bucket_name, path, title, region)
        
    else:
        path = f'{local}/{path}'
        return save_response_json_local(data, path, title)