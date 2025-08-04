import os
import sys
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta

from ..scrapers.sofascore_scraper_curl import SofaScoreScraper
from ..utils.save_response_json import save_response_to_json, save_response_json_to_s3
from ..utils.save_dataframe_csv import save_dataframe_to_csv

def get_sports():
    """
    Busca os dados dos esportes disponíveis.
    """
    url = f"https://www.sofascore.com/api/v1/sport/-10800/event-count"
    scraper = SofaScoreScraper()
    return scraper._make_request(url)

def extract_sports():
    '''
    Extrair a resposta do servidor ao scraper dos Esportes

    :param scraper: Classe do SofaScoreScraper
    :return: A resposta do servidor ao scraper Sports
    '''
    try:
        response_sports = get_sports()
        if not isinstance(response_sports, dict):
            logging.error(f"Erro na resposta dos esportes: {response_sports}")
            return None
    except Exception as e:
        logging.exception(f"Erro ao buscar dados dos esportes: {str(e)}")
        response_sports = None
    
    return response_sports

def transform_sports(response_sports, datetime_now):
    '''
    Transformar os dados do response_sports em um dataframe

    :param response_sports: A resposta do servidor ao scraper Sports
    :return: Um dataframe com os esportes
    '''
    list_live = []
    list_total = []
    list_sports = []
    list_updated_at = []
    
    if response_sports:
        for sport_name, data_sport in response_sports.items():
            if isinstance(data_sport, dict) and 'live' in data_sport and 'total' in data_sport:
                list_sports.append(sport_name)
                list_live.append(data_sport['live'])
                list_total.append(data_sport['total'])
                list_updated_at.append(datetime_now)
            else:
                logging.warning(f"Dados inválidos para o esporte {sport_name}: {data_sport}")
    else:
        logging.warning("Resposta dos esportes está vazia ou inválida.")

    df_sports = pd.DataFrame({
        'sport': list_sports,
        'live': list_live,
        'total': list_total,
        'updated_at': list_updated_at
    })

    return df_sports
