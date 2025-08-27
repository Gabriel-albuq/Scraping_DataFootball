from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from src.scraping_datafootball.steps.s005_steps_rounds import extract_rounds, transform_rounds

from src.scraping_datafootball.utils.check_existencia_s3 import check_existencia_s3
from src.scraping_datafootball.utils.save_response_json import save_response_to_json, save_response_json_to_s3
from src.scraping_datafootball.utils.load_csv import load_csv_from_s3
from src.scraping_datafootball.utils.load_response_json import load_response_json, load_response_json_from_s3
from src.scraping_datafootball.utils.save_dataframe_csv import save_dataframe_csv_to_s3

from config.p000_input_dict import input_dict, save_location, source, bucket_name, region_name

# Tirando duplicidade
input_dict_original = input_dict.copy()
combinations_seen = set()
input_dict = []
for item in input_dict_original:
    # Cria uma tupla com os quatro valores que definem a unicidade
    combination = (item['sport'], item['country'], item['tournament'], item['year'])
    
    if combination not in combinations_seen:
        combinations_seen.add(combination)
        input_dict.append(item)

# Inputs
dag_path = '05-rounds'

@dag(
    dag_id="sofascore_scrapper_05_rounds",
    description="Extração de dados de Rodadas do SofaScore",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['scraping', 'seasons']
)
def dag_sofascore_scrapper_05_rounds():
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
    def consolidar_listas(input_dict):
        lista_consolidada = [item for sublist in input_dict for item in sublist]
        return lista_consolidada

    @task
    def verificar_existencia(input_dict):
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        sport = input_dict['sport']
        country = input_dict['country']
        tournament = input_dict['tournament']
        year = input_dict['year']
        season = input_dict['season']
        title = f"rounds_{sport}_{country}_{tournament}_{season}"

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
            "season": season
        }

    @task
    def extrair_e_salvar_dados(input_dict, forcar = False):
        """
        Extrai os dados de Rounds e salva na camada Bronze no S3.
        Retorna o caminho completo do arquivo salvo.
        """
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        path_bronze = input_dict['path_bronze']
        exist_bronze = input_dict['exist_bronze']
        path_silver = input_dict['path_silver']
        exist_silver = input_dict ['exist_silver']
        title = input_dict['title']
        sport = input_dict['sport']
        country = input_dict['country']
        tournament = input_dict['tournament']
        year = input_dict['year']
        season = input_dict['season']

        if (exist_bronze == False or forcar == True):
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
                    "exist_bronze": exist_bronze,
                    "path_silver": path_silver,
                    "exist_silver": exist_silver,
                    "title": title,
                    "sport": sport,
                    "country": country,
                    "tournament": tournament,
                    "year": year,
                    "season": season
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
                    "season": season
            }

    @task
    def transformar_e_salvar_dados(input_dict, forcar = False):
        """
        Lê o arquivo da camada Bronze, transforma os dados e salva na camada Silver.
        """
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        path_bronze = input_dict['path_bronze']
        exist_bronze = input_dict ['exist_bronze']
        path_silver = input_dict['path_silver']
        exist_silver = input_dict ['exist_silver']
        title = input_dict['title']
        sport = input_dict['sport']
        country = input_dict['country']
        tournament = input_dict['tournament']
        year = input_dict['year']
        season = input_dict['season']

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

                    else:
                        print("O DataFrame está vazio. Nada a salvar.")
                else:
                    print(f"Erro: json_data para o título '{title}' é None. Pulando a transformação.")

            except Exception as e:
                error_message = f"Erro ao transformar e salvar os dados dos torneios: {e}"
                raise RuntimeError(error_message)

        else:
            print("O arquivo já existe na camada silver")

    obter_seasons = obter_season_id.expand(input_dict=input_dict)
    consolidar_seasons = consolidar_listas(obter_seasons)
    verificacao = verificar_existencia.expand(input_dict=consolidar_seasons)
    extracao = extrair_e_salvar_dados.partial(forcar=False).expand(input_dict=verificacao)
    transformacao = transformar_e_salvar_dados.partial(forcar=False).expand(input_dict=extracao)

    disparar_proxima_dag = TriggerDagRunOperator(
        task_id='trigger_sofascore_scrapper_06_matches',
        trigger_dag_id='sofascore_scrapper_06_matches',
         conf={},
    )

    obter_seasons >> verificacao >> extracao >> transformacao >> disparar_proxima_dag

dag_sofascore_scrapper_05_rounds()