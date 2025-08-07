from airflow.decorators import dag, task
from datetime import datetime
import os

from src.scraping_datafootball.steps.s004_steps_seasons import extract_seasons, transform_seasons

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
        "country": "13",
        "tournament": '325'
    },  
    {
        "sport": "football",
        "country": "13",
        "tournament": '373'
    }
]

#'14659', # Acreano
#'10294', # Alagoano
#'13668', # Amapazão
#'11702', # Amazonense
#'374', # Baiano
#'325', # BrasileirÃ£o Betano
#'390', # BrasileirÃ£o Série B
#'11682', # Brasiliense
#'14650', # Capixaba
#'92', #	Carioca
#'376', # Catarinense
#'378', # Cearense
#'373' # Copa Betano do Brasil
#'1596', # Copa do Nordeste
#'377', # GaÃºcho
#'381', # Goiano
#'11664', # Maranhense
#'11669', # Paraense
#'10295', # Paraibano
#'382', # Paranaense
#'372', # Paulista Série A1
#'380', # Pernambucano
#'13353', # Piauiense
#'11663', # Potiguar, 1 Divisão 
#'14658', # Rondoniense
#'14733', # Roraimense
#'11665', # Sergipano
#'11679', # Sul-Mato-Grossense
#'14602', # Supercopa do Brasil
#'14686', # Tocantinense

@dag(
    dag_id="sofascore_scrapper_04_seasons",
    description="Extração de dados de Temporadas do SofaScore",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['scraping', 'seasons']
)
def pipeline():
    @task
    def extrair_e_salvar_dados(input_dict):
        """
        Extrai os dados de Temporadas e salva na camada Bronze no S3.
        Retorna o caminho completo do arquivo salvo.
        """
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        sport = input_dict['sport']
        country = input_dict['country']
        tournament = input_dict['tournament']

        title = f"seasons_{sport}_{country}_{tournament}"
        path_bronze = f'sofascore/01-bronze/04-seasons'
        
        response_seasons = extract_seasons(tournament)
        if response_seasons:
            save_response_json_to_s3(
                data=response_seasons, 
                bucket_name=bucket_name,
                path=path_bronze,
                title=title,
                region=region_name
            )
            return {
                "path_bronze": path_bronze,
                "title_bronze": title,
                "tournament": tournament
            }
        else:
            error_message = "Não foi possível extrair dados das temporadas. A extração retornou um valor vazio ou nulo."
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
            tournaments = extract_dict['tournament']

            print(f"Lendo dados do S3 em: s3://{bucket_name}/{path_bronze}/{title}")
            json_data = load_response_json_from_s3(
                bucket_name=bucket_name,
                path=path_bronze,
                title=title,
                region=region_name
            )

            path_silver = f'sofascore/02-silver/04-seasons'
            df_data = transform_seasons(json_data, datetime_now)

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