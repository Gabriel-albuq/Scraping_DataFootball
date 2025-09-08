import os
import sys
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta

from ..scrapers.sofascore_scraper_curl import SofaScoreScraper

def get_countries(sport):
    """
    Busca os dados de todos os países disponíveis.
    """
    scraper = SofaScoreScraper()
    url = f"https://www.sofascore.com/api/v1/config/default-unique-tournaments/BR/{sport}"
    return scraper._make_request(url)

def extract_countries(sport):
    '''
    Extrair a resposta do servidor ao scraper dos Países

    :param sport: Nome do esporte para buscar os países
    :return: A resposta do servidor ao scraper dos Países
    '''
    try:
        response_countries = [{
            'sport': sport,
            'countries': get_countries(sport)
        }]
        if not isinstance(response_countries[0]['countries'], dict):
            logging.error(f"Erro ao buscar países para o esporte {sport}: resposta inválida.")
            return None
    except Exception as e:
        logging.error(f"Erro ao buscar dados dos países para o esporte {sport}: {str(e)}")
        response_countries = None
    
    return response_countries

def transform_countries(response_countries, datetime_now):
    '''
    Transformar os dados do response_countries em um dataframe

    :param response_countries: A resposta do servidor ao scraper countries
    :param datetime_now: Data e hora atuais
    :return: Um dataframe com os Países
    '''
    list_sport = []
    list_country_name = []
    list_sport_country_id = []
    list_updated_at = []
    
    if response_countries:
        for sport_countries in response_countries:
            if 'countries' in sport_countries:
                for country in sport_countries['countries'].get('uniqueTournaments', []):
                    if 'category' in country and 'name' in country['category'] and 'id' in country['category']:
                        list_sport.append(sport_countries['sport'])
                        list_country_name.append(country['category']['name'])
                        list_sport_country_id.append(country['category']['id'])
                        list_updated_at.append(datetime_now)
                    else:
                        logging.warning(f"Dados faltando ou inválidos para o país: {country}")
            else:
                logging.warning(f"Dados de países ausentes para o esporte: {sport_countries['sport']}")
    else:
        logging.warning("Resposta dos países está vazia ou inválida.")

    # Criar DataFrame
    df_countries = pd.DataFrame({
        'sport_country_id': list_sport_country_id,
        'sport': list_sport,
        'country_name': list_country_name,
        'updated_at': list_updated_at
    })

    return df_countries