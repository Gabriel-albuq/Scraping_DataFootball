from airflow.decorators import dag, task
from datetime import datetime
import os

from src.scraping_datafootball.steps.s002_steps_countries import extract_countries
from src.scraping_datafootball.utils.save_response_json import save_response_to_json, save_response_json_to_s3

# Inputs
save_location = 's3'
list_sports = [
                "football",
                "futsal"
]

@dag(
    dag_id="sofascore_scrapper_countries",
    description="Extração de dados de Países do SofaScore",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['scraping', 'countries']
)
def pipeline():
    @task
    def extrair_e_salvar_dados(save_location: str, sport: str):
        """
        Função que será executada pelo PythonOperator para extrair e salvar os dados.
        """
        save_path = 'data/outputs'
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        # A more direct and reliable way to create the title and path
        title = f"countries_{sport}"
        path = os.path.join(save_path, "raw", datetime_now, sport)
        
        response_countries = extract_countries(sport)
        
        if response_countries:
            if save_location.lower() == 's3':
                s3_path = f'sofascore/sports/{sport}/00.extracts'
                save_response_json_to_s3(
                    data=response_countries, 
                    bucket_name='gaa-datafootball', 
                    path=s3_path, 
                    title=title, 
                    region='us-east-1'
                )
            else:
                save_response_to_json(response_countries, path, title)
        else:
            error_message = f"Não foi possível extrair dados para o esporte: {sport}. A extração retornou um valor vazio ou nulo."
            raise ValueError(error_message)
    # Use .partial() for the fixed argument and .expand() for the mapped one.
    extrair_e_salvar_dados.partial(save_location=save_location).expand(sport=list_sports)

pipeline()