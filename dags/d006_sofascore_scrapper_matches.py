from airflow.decorators import dag, task
from datetime import datetime
import os

from src.scraping_datafootball.steps.s006_steps_matches import extract_matches, transform_matches

from src.scraping_datafootball.utils.check_existencia_s3 import check_existencia_s3
from src.scraping_datafootball.utils.save_response_json import save_response_to_json, save_response_json_to_s3
from src.scraping_datafootball.utils.load_csv import load_csv_from_s3
from src.scraping_datafootball.utils.load_response_json import load_response_json, load_response_json_from_s3
from src.scraping_datafootball.utils.save_dataframe_csv import save_dataframe_csv_to_s3

# Inputs
save_location = 's3'
source = 'sofascore'
dag_path = '06-matches'
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
    dag_id="sofascore_scrapper_06_matches",
    description="Extração de dados de Partidas do SofaScore",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['scraping', 'matches']
)
def pipeline():
    @task
    def obter_season_id(input_dict):
        sport = input_dict['sport']
        country = input_dict['country']
        tournament = input_dict['tournament']
        year = input_dict['year']

        # Seasons: 02-silver
        layer = '02-silver'
        path_seasons = f'{source}/{layer}/04-seasons'
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

        novo_input_dict = [
            {
                "sport": sport,
                "country": country,
                "tournament": tournament,
                "year": year,
                "season": season[0] # Extrai o ID da temporada do array NumPy
            }
            for season in list_seasons
        ]

        if not list_seasons:
            raise ValueError(f"Não foram encontradas temporadas para o ano {year}.")

        return novo_input_dict

    @task
    def consolidar_listas(novo_input_dict):
        lista_consolidada = [item for sublist in novo_input_dict for item in sublist]
        return lista_consolidada
    
    @task
    def obter_round_slug(novo_input_dict):
        sport = novo_input_dict['sport']
        country = novo_input_dict['country']
        tournament = novo_input_dict['tournament']
        year = novo_input_dict['year']
        season = novo_input_dict['season']

        # Seasons: 02-silver
        layer = '02-silver'
        path_rounds = f'{source}/{layer}/05-rounds'
        title_rounds = f"rounds_{sport}_{country}_{tournament}_{season}"

        print(f"Lendo dados do S3 em: s3://{bucket_name}/{path_rounds}/{title_rounds}")
        df = load_csv_from_s3(
            bucket_name=bucket_name,
            path=path_rounds,
            title=title_rounds,
            region=region_name,
        )

        df['season_id'] = df['season_id'].astype(str)
        df['round'] = df['round'].astype(str)
        df['slug'] = df['slug'].astype(str)
        df = df[df['season_id'] == season]
        list_values = [tuple(map(str, raw)) for raw in df[['round', 'slug']].drop_duplicates().to_numpy()]
        
        novo_input_dict = [
            {
                "sport": sport,
                "country": country,
                "tournament": tournament,
                "year": year,
                "season": season,
                "round": value[0],
                "slug": value[1]
            }
            for value in list_values
        ]

        if not list_values:
            raise ValueError(f"Não foram encontradas rodadas para a temporada {season}.")

        return novo_input_dict

    @task
    def verificar_existencia(input_dict):
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        sport = input_dict['sport']
        country = input_dict['country']
        tournament = input_dict['tournament']
        year = input_dict['year']
        season = input_dict['season']
        round = input_dict['round']
        slug = input_dict['slug']
        title = f"matches_{sport}_{country}_{tournament}_{season}_{round}_{slug}"

        # 01-bronze
        layer = '01-bronze'
        path_bronze = f'{source}/{layer}/{dag_path}'
        exist_bronze = check_existencia_s3(
                            bucket_name=bucket_name,
                            path=path_bronze,
                            title=title,
                            region=region_name
                        )

        # 02-silver
        layer = '02-silver'
        path_silver = f'{source}/{layer}/{dag_path}'
        exist_silver = check_existencia_s3(
                            bucket_name=bucket_name,
                            path=path_silver,
                            title=title,
                            region=region_name
                        )
        
        return {
            "path_bronze": path_bronze,
            "exist_bronze": exist_bronze,
            "path_silver": path_silver,
            "exist_silver": exist_silver,
            "title": title,
            "sport": sport,
            "country": country,
            "tournament": tournament,
            "year": year,
            "season": season,
            "round": round,
            "slug": slug
        }

    @task
    def extrair_e_salvar_dados(verificacao_dict, forcar = False):
        """
        Extrai os dados de Rounds e salva na camada Bronze no S3.
        Retorna o caminho completo do arquivo salvo.
        """
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        path_bronze = verificacao_dict['path_bronze']
        exist_bronze = verificacao_dict['exist_bronze']
        path_silver = verificacao_dict['path_silver']
        exist_silver = verificacao_dict ['exist_silver']
        title = verificacao_dict['title']
        sport = verificacao_dict['sport']
        country = verificacao_dict['country']
        tournament = verificacao_dict['tournament']
        year = verificacao_dict['year']
        season = verificacao_dict['season']
        round = verificacao_dict['round']
        slug = verificacao_dict['slug']

        if (exist_bronze == False or forcar == True):
            response_seasons = extract_matches(tournament, season, round, slug)
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
                    "exist_bronze": exist_bronze,
                    "path_silver": path_silver,
                    "exist_silver": exist_silver,
                    "title": title,
                    "sport": sport,
                    "country": country,
                    "tournament": tournament,
                    "year": year,
                    "season": season,
                    "round": round,
                    "slug": slug
                }
            else:
                error_message = "Não foi possível extrair dados das rodadas. A extração retornou um valor vazio ou nulo."
                raise ValueError(error_message)

        else:
            print("O arquivo já existe na camada bronze")
            return {
                    "path_bronze": path_bronze,
                    "exist_bronze": exist_bronze,
                    "path_silver": path_silver,
                    "exist_silver": exist_silver,
                    "title": title,
                    "sport": sport,
                    "country": country,
                    "tournament": tournament,
                    "year": year,
                    "season": season,
                    "round": round,
                    "slug": slug
            }

    @task
    def transformar_e_salvar_dados(extract_dict, forcar = False):
        """
        Lê o arquivo da camada Bronze, transforma os dados e salva na camada Silver.
        """
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        path_bronze = extract_dict['path_bronze']
        exist_bronze = extract_dict ['exist_bronze']
        path_silver = extract_dict['path_silver']
        exist_silver = extract_dict ['exist_silver']
        title = extract_dict['title']
        sport = extract_dict['sport']
        country = extract_dict['country']
        tournament = extract_dict['tournament']
        year = extract_dict['year']
        season = extract_dict['season']
        round = extract_dict['round']
        slug = extract_dict['slug']

        if (exist_silver == False or forcar == True):
            try:
                print(f"Lendo dados do S3 em: s3://{bucket_name}/{path_bronze}/{title}")
                json_data = load_response_json_from_s3(
                    bucket_name=bucket_name,
                    path=path_bronze,
                    title=title,
                    region=region_name
                )

                if json_data is not None:
                    df_data = transform_matches(json_data, datetime_now)
                    if not df_data.empty:
                        save_dataframe_csv_to_s3(
                            df=df_data,
                            bucket_name=bucket_name,
                            path=path_silver,
                            title=title,
                            region=region_name
                        )

                        print(f"Salvando dados transformados no S3 em: s3://{bucket_name}/{path_silver}/{title}.csv")

                    else:
                        print("O DataFrame está vazio. Nada a salvar.")
                else:
                    print(f"Erro: json_data para o título '{title}' é None. Pulando a transformação.")

            except Exception as e:
                error_message = f"Erro ao transformar e salvar os dados dos torneios: {e}"
                raise RuntimeError(error_message)

        else:
            print("O arquivo já existe na camada silver")

    novo_input_dict = obter_season_id.expand(input_dict=input_dict)
    novo_input_dict = consolidar_listas(novo_input_dict)
    novo_input_dict = obter_round_slug.expand(novo_input_dict=novo_input_dict)
    novo_input_dict = consolidar_listas(novo_input_dict)
    verificacao_dict = verificar_existencia.expand(input_dict=novo_input_dict)
    extract_dict = extrair_e_salvar_dados.partial(forcar=False).expand(verificacao_dict=verificacao_dict)
    transformar_e_salvar_dados.partial(forcar=False).expand(extract_dict=extract_dict)

pipeline()  