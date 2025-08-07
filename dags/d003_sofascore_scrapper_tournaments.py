from airflow.decorators import dag, task
from datetime import datetime
import os

from src.scraping_datafootball.steps.s003_steps_tournaments import extract_tournaments, transform_tournaments

from src.scraping_datafootball.utils.save_response_json import save_response_to_json, save_response_json_to_s3
from src.scraping_datafootball.utils.load_response_json import load_response_json, load_response_json_from_s3
from src.scraping_datafootball.utils.save_dataframe_csv import save_dataframe_csv_to_s3

# Inputs
save_location = 's3'
bucket_name = 'gaa-datafootball'
region_name = 'us-east-1'
input_dict = [
    {
        "sport": "football",
        "country": "13"
    }
]

@dag(
    dag_id="sofascore_scrapper_03_tournaments",
    description="Extração de dados de Torneios do SofaScore",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['scraping', 'tournaments']
)
def pipeline():
    @task
    def extrair_e_salvar_dados(input_dict):
        """
        Extrai os dados de Torneios e salva na camada Bronze no S3.
        Retorna o caminho completo do arquivo salvo.
        """
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        sport = input_dict['sport']
        country = input_dict['country']

        title = f"tournaments_{sport}_{country}"
        path_bronze = f'sofascore/01-bronze/03-tournaments'
        
        response_tournaments = extract_tournaments(country)
        if response_tournaments:
            save_response_json_to_s3(
                data=response_tournaments, 
                bucket_name=bucket_name,
                path=path_bronze,
                title=title,
                region=region_name
            )
            return {
                "path_bronze": path_bronze,
                "title_bronze": title,
                "country": country
            }
        else:
            error_message = "Não foi possível extrair dados dos países. A extração retornou um valor vazio ou nulo."
            raise ValueError(error_message)
        
    @task
    def transformar_e_salvar_dados(extract_dict):
        """
        Lê o arquivo da camada Bronze, transforma os dados e salva na camada Silver.
        """
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        try:
            path_bronze = extract_dict['path_bronze']
            title = extract_dict['title_bronze']
            country = extract_dict['country']

            print(f"Lendo dados do S3 em: s3://{bucket_name}/{path_bronze}/{title}")
            json_data = load_response_json_from_s3(
                bucket_name=bucket_name,
                path=path_bronze,
                title=title,
                region=region_name
            )

            path_silver = f'sofascore/02-silver/03-tournaments'
            df_data = transform_tournaments(json_data, datetime_now)

            if not df_data.empty:
                save_dataframe_csv_to_s3(
                df=df_data,
                bucket_name=bucket_name,
                path=path_silver,
                title=title,
                region=region_name
            )

            print(f"Salvando dados transformados no S3 em: s3://{bucket_name}/{path_silver}/{title}.csv")

        except Exception as e:
            error_message = f"Erro ao transformar e salvar os dados dos torneios: {e}"
            raise RuntimeError(error_message)

    extract_dict = extrair_e_salvar_dados.partial().expand(input_dict=input_dict)
    transformar_e_salvar_dados.expand(extract_dict=extract_dict)

pipeline()