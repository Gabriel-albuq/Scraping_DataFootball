import boto3
import json
import os
from dotenv import load_dotenv
from pathlib import Path

# Carrega o .env
load_dotenv(dotenv_path=Path('.') / '.env')

# Dados de teste para upload
data_de_teste = {
    "status": "sucesso",
    "mensagem": "Este é um arquivo JSON de teste.",
    "data_de_criacao": "2025-08-04T17:40:00Z"
}

# Configurações do S3
bucket_name = 'gaa-datafootball'
file_key = 'testes/teste_de_credencial.json'
region = os.getenv('AWS_REGION')

def testar_upload_s3():
    print("Iniciando o teste de upload para o S3...")

    try:
        s3_client = boto3.client(
            's3',
            region_name=region,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )

        json_data = json.dumps(data_de_teste, indent=2)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json_data,
            ContentType='application/json'
        )

        print(f"\n--- SUCESSO! ---")
        print(f"Arquivo JSON de teste salvo com sucesso em s3://{bucket_name}/{file_key}")
        
    except Exception as e:
        print(f"\n--- ERRO! ---")
        print(f"Ocorreu um erro durante o upload: {e}")
        raise e

if __name__ == "__main__":
    testar_upload_s3()
