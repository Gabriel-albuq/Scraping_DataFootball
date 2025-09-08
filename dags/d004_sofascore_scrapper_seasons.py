from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from src.scraping_datafootball.steps.s004_steps_seasons import extract_seasons, transform_seasons

from src.scraping_datafootball.utils.check_existencia import check_existencia
from src.scraping_datafootball.utils.save_response_json import save_response_json
from src.scraping_datafootball.utils.load_response_json import load_response_json
from src.scraping_datafootball.utils.save_dataframe_csv import save_dataframe_csv

from config.p000_input_dict import input_dict, save_location, source, bucket_name, region_name

# Tirando duplicidade
input_dict_original = input_dict.copy()
combinations_seen = set()
input_dict = []
for item in input_dict_original:
    # Cria uma tupla com os valores que definem a unicidade
    combination = (item['sport'], item['country'], item['tournament'])
    if combination not in combinations_seen:
        combinations_seen.add(combination)
        input_dict.append(item)

# Inputs
dag_path = '04-seasons'

@dag(
    dag_id="sofascore_scrapper_04_seasons",
    description="Extração de dados de Temporadas do SofaScore",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['scraping', 'seasons']
)
def dag_sofascore_scrapper_04_seasons():
    @task
    def verificar_existencia(input_dict):
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        sport = input_dict['sport']
        country = input_dict['country']
        tournament = input_dict['tournament']
        title = f"seasons_{sport}_{country}_{tournament}"

        # 01-bronze
        layer = '01-bronze'
        path_bronze = f'{source}/{layer}/{dag_path}'
        exist_bronze = check_existencia(
                            local = save_location,
                            path=path_bronze,
                            title=title,
                            region=region_name,
                            bucket_name=bucket_name,
                        )

        # 02-silver
        layer = '02-silver'
        path_silver = f'{source}/{layer}/{dag_path}'
        exist_silver = check_existencia(
                            local = save_location,
                            path=path_silver,
                            title=title,
                            region=region_name,
                            bucket_name=bucket_name,
                        )
        
        return {
            "path_bronze": path_bronze,
            "exist_bronze": exist_bronze,
            "path_silver": path_silver,
            "exist_silver": exist_silver,
            "title": title,
            "sport": sport,
            "country": country,
            "tournament": tournament
        }

    @task
    def extrair_e_salvar_dados(input_dict, forcar = False):
        """
        Extrai os dados de Temporadas e salva na camada Bronze no S3.
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

        if (exist_bronze == False or forcar == True):
            response_seasons = extract_seasons(tournament)
            if response_seasons:
                save_response_json(
                    local = save_location,
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
                    "tournament": tournament
                }
            else:
                error_message = "Não foi possível extrair dados das temporadas. A extração retornou um valor vazio ou nulo."
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
                "tournament": tournament
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

        if (exist_silver == False or forcar == True):
            try:
                print(f"Lendo dados do S3 em: s3://{bucket_name}/{path_bronze}/{title}")
                json_data = load_response_json(
                    local = save_location,
                    bucket_name=bucket_name,
                    path=path_bronze,
                    title=title,
                    region=region_name
                )

                if json_data is not None:
                    df_data = transform_seasons(json_data, datetime_now)
                    if not df_data.empty:
                        save_dataframe_csv(
                            local = save_location,
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

    verificacao = verificar_existencia.expand(input_dict=input_dict)
    extracao = extrair_e_salvar_dados.partial(forcar=False).expand(input_dict=verificacao)
    transformacao = transformar_e_salvar_dados.partial(forcar=False).expand(input_dict=extracao)

    disparar_proxima_dag = TriggerDagRunOperator(
        task_id='trigger_sofascore_scrapper_05_rounds',
        trigger_dag_id='sofascore_scrapper_05_rounds',
         conf={},
    )

    verificacao >> extracao >> transformacao >> disparar_proxima_dag

dag_sofascore_scrapper_04_seasons()  