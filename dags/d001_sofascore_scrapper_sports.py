from airflow.decorators import dag, task
from datetime import datetime
import os

from src.scraping_datafootball.steps.s001_steps_sports import extract_sports
from src.scraping_datafootball.utils.save_response_json import save_response_to_json, save_response_json_to_s3

# Inputs
save_location = 's3'
list_sports = [
                "football",
                "futsal"
]

@dag(
    dag_id="sofascore_scrapper_sports",
    description="Extração de dados de Esportes do SofaScore",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['scraping', 'sports']
)

def pipeline():
    @task
    def extrair_e_salvar_dados(save_location):
        """
        Função que será executada pelo PythonOperator para extrair e salvar os dados.
        """
        save_path = 'data/outputs'
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        title = f"sports"
        title_path = title.split(" - ")[0]
        title_datetime = title.rsplit(" - ", 1)[-1]
        
        path = os.path.join(save_path, "raw", title_datetime, title_path)
        
        response_sports = extract_sports()
        if response_sports:
            s3_path = 'sofascore/sports/00.extracts'
            if save_location in ('s3', 'S3'):
                save_response_json_to_s3(data = response_sports, bucket_name = 'gaa-datafootball', path= s3_path, title = title, region='us-east-1')
            else:
                save_response_to_json(response_sports, path, title)
        else:
            error_message = f"Não foi possível extrair dados dos esportes.. A extração retornou um valor vazio ou nulo."
            raise ValueError(error_message)

    t1 = extrair_e_salvar_dados('s3')

    t1

pipeline()