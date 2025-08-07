from airflow.decorators import dag, task
from datetime import datetime
import os

from src.scraping_datafootball.steps.s005_steps_rounds import extract_rounds, transform_rounds

from src.scraping_datafootball.utils.save_response_json import save_response_to_json, save_response_json_to_s3
from src.scraping_datafootball.utils.load_csv import load_csv_from_s3
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
        "tournament": '325',
        "year": '2024'
    },  
    {
        "sport": "football",
        "country": "13",
        "tournament": '373',
        "year": '2024'
    }
]

@dag(
    dag_id="sofascore_scrapper_05_rounds",
    description="Extração de dados de Temporadas do SofaScore",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['scraping', 'seasons']
)
def pipeline():
    @task
    def obter_season_id(input_dict):
        sport = input_dict['sport']
        country = input_dict['country']
        tournament = input_dict['tournament']
        year = input_dict['year']

        path_seasons = f'sofascore/02-silver/04-seasons'
        title_seasons = f'seasons_{sport}_{country}_{tournament}'

        print(f"Lendo dados do S3 em: s3://{bucket_name}/{path_seasons}/{title_seasons}")
        df = load_csv_from_s3(
            bucket_name=bucket_name,
            path=path_seasons,
            title=title_seasons,
            region=region_name,
        )

        df['season_year'] = df['season_year'].astype(str)
        df = df[df['season_year'] == year]
        list_seasons = [tuple(map(str, raw)) for raw in df[['season_id']].drop_duplicates().to_numpy()]

        if not list_seasons:
            raise ValueError(f"Não foram encontradas temporadas para o ano {year}.")


        novo_dict = [
            {
                "season": season[0],  # Extrai o ID da temporada do array NumPy
                "sport": sport,
                "country": country,
                "tournament": tournament,
                "year": year
            }
            for season in list_seasons
        ]

        print(novo_dict)
        return novo_dict

    @task
    def consolidar_listas(listas_de_seasons):
        lista_consolidada = [item for sublist in listas_de_seasons for item in sublist]
        return lista_consolidada

    @task
    def extrair_e_salvar_dados(input_dict):
        """
        Extrai os dados de Rounds e salva na camada Bronze no S3.
        Retorna o caminho completo do arquivo salvo.
        """
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        sport = input_dict['sport']
        country = input_dict['country']
        tournament = input_dict['tournament']
        season = input_dict['season']

        title = f"rounds_{sport}_{country}_{tournament}_{season}"
        path_bronze = f'sofascore/01-bronze/05-rounds'
        
        response_seasons = extract_rounds(tournament, season)
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
                "season": season
            }
        else:
            error_message = "Não foi possível extrair dados das rodadas. A extração retornou um valor vazio ou nulo."
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

            print(f"Lendo dados do S3 em: s3://{bucket_name}/{path_bronze}/{title}")
            json_data = load_response_json_from_s3(
                bucket_name=bucket_name,
                path=path_bronze,
                title=title,
                region=region_name
            )

            path_silver = f'sofascore/02-silver/04-seasons'
            df_data = transform_rounds(json_data, datetime_now)

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

    lista_de_seasons_por_torneio = obter_season_id.expand(input_dict=input_dict)
    lista_consolidada = consolidar_listas(lista_de_seasons_por_torneio)
    extracted_data = extrair_e_salvar_dados.expand(input_dict=lista_consolidada)
    transformar_e_salvar_dados.expand(extract_dict=extracted_data)

pipeline()