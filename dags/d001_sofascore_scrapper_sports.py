from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from src.scraping_datafootball.steps.s001_steps_sports import extract_sports, transform_sports

from src.scraping_datafootball.utils.check_existencia import check_existencia
from src.scraping_datafootball.utils.save_response_json import save_response_json
from src.scraping_datafootball.utils.load_response_json import load_response_json
from src.scraping_datafootball.utils.save_dataframe_csv import save_dataframe_csv

from config.p000_input_dict import input_dict, save_location, source, bucket_name, region_name

# Inputs
dag_path = '01-sports'

@dag(
    dag_id="sofascore_scrapper_01_sports",
    description="Extração e Transformação de dados de Esportes do SofaScore",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['scraping', 'sports']
)
def dag_sofascore_scrapper_01_sports():
    @task
    def verificar_existencia():
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        title = f"sports"

        # 01-bronze
        layer = '01-bronze'
        path_bronze = f'{source}/{layer}/{dag_path}'
        exist_bronze = check_existencia(
                            local = save_location,
                            path=path_bronze,
                            title=title,
                            extensao='.json',
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
                            extensao='.csv',
                            region=region_name,
                            bucket_name=bucket_name,
                        )
            
        return {
            "path_bronze": path_bronze,
            "exist_bronze": exist_bronze,
            "path_silver": path_silver,
            "exist_silver": exist_silver,
            "title": title
        }

    @task
    def extrair_e_salvar_dados(input_dict, forcar = False):
        """
        Extrai os dados de esportes e salva na camada Bronze no S3.
        Retorna o caminho completo do arquivo salvo.
        """
        datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        path_bronze = input_dict['path_bronze']
        exist_bronze = input_dict['exist_bronze']
        path_silver = input_dict['path_silver']
        exist_silver = input_dict ['exist_silver']
        title = input_dict['title']

        print(save_location)
        
        if (exist_bronze == False or forcar == True):
            response_sports = extract_sports()
            if response_sports:
                save_response_json(
                    local = save_location,
                    data=response_sports,
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
                    "title": title
                }
            else:
                error_message = "Não foi possível extrair dados dos esportes. A extração retornou um valor vazio ou nulo."
                raise ValueError(error_message)
            
        else:
            print("O arquivo já existe na camada bronze")
            return {
                "path_bronze": path_bronze,
                "exist_bronze": exist_bronze,
                "path_silver": path_silver,
                "exist_silver": exist_silver,
                "title": title
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

        if (exist_silver == False or forcar == True):
            try:
                print(f"Lendo dados da camada bronze://{path_bronze}/{title}")
                json_data = load_response_json(
                    local = save_location,
                    bucket_name=bucket_name,
                    path=path_bronze,
                    title=title,
                    region=region_name
                )
                
                if json_data is not None:
                    df_data = transform_sports(json_data, datetime_now)
                    if not df_data.empty:
                        save_dataframe_csv(
                            local = save_location,
                            df=df_data,
                            bucket_name=bucket_name,
                            path=path_silver,
                            title=title,
                            region=region_name
                        )
                        print(f"Salvando dados transformados://{path_silver}/{title}.csv")

                    else:
                        print("O DataFrame está vazio. Nada a salvar.")
                else:
                    print(f"Erro: json_data para o título '{title}' é None. Pulando a transformação.")

            except Exception as e:
                error_message = f"Erro ao transformar e salvar os dados dos esportes: {e}"
                raise RuntimeError(error_message)
            
        else:
            print("O arquivo já existe na camada silver")

    verificacao = verificar_existencia()
    extracao = extrair_e_salvar_dados(verificacao)
    transformacao = transformar_e_salvar_dados(extracao)

    disparar_proxima_dag = TriggerDagRunOperator(
        task_id='trigger_sofascore_scrapper_02_countries',
        trigger_dag_id='sofascore_scrapper_02_countries',
         conf={},
    )

    verificacao >> extracao >> transformacao >> disparar_proxima_dag
    
dag_sofascore_scrapper_01_sports()