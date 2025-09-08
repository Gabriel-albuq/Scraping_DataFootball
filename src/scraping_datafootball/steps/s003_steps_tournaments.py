import os
import sys
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta

from ..scrapers.sofascore_scraper_curl import SofaScoreScraper

def get_tournaments(sport_country_id):
    """
    Busca os dados dos torneios disponíveis no esporte.
    """
    scraper = SofaScoreScraper()
    url = f"https://www.sofascore.com/api/v1/category/{sport_country_id}/unique-tournaments"
    return scraper._make_request(url)

def extract_tournaments(sport_country_id):
    '''
    Extrair a resposta do servidor ao scraper dos Torneios

    :param sport_country_id: ID do país e esporte para buscar os torneios
    :return: A resposta do servidor ao scraper dos Torneios
    '''
    try:
        response_tournaments = [{
            'sport_country_id': sport_country_id,
            'tournaments': get_tournaments(sport_country_id)
        }]
        
        if not isinstance(response_tournaments[0]['tournaments'], dict):
            logging.error(f"Erro ao buscar torneios para o país/ID {sport_country_id}: resposta inválida.")
            return None
    except Exception as e:
        logging.error(f"Erro ao buscar dados dos torneios para o país/ID {sport_country_id}: {str(e)}")
        response_tournaments = None
    
    return response_tournaments

def transform_tournaments(response_tournaments, datetime_now):
    '''
    Transformar os dados do response_tournaments em um dataframe

    :param response_tournaments: A resposta do servidor ao scraper dos torneios
    :param datetime_now: Data e hora atuais
    :return: Um dataframe com os Torneios
    '''
    list_sport_country_id = []
    list_tournament_name = []
    list_tournament_id = []
    list_category_name = []
    list_updated_at = []
    
    if response_tournaments:
        for sport_tournaments in response_tournaments:
            if 'tournaments' in sport_tournaments:
                for group in sport_tournaments['tournaments'].get('groups', []):
                    for tournament in group.get('uniqueTournaments', []):
                        if 'category' in tournament and 'name' in tournament['category'] and 'id' in tournament['category']:
                            list_sport_country_id.append(sport_tournaments['sport_country_id'])
                            list_tournament_name.append(tournament['name'])
                            list_tournament_id.append(tournament['id'])
                            list_category_name.append(tournament['category']['name'])
                            list_updated_at.append(datetime_now)
                        else:
                            logging.warning(f"Dados faltando ou inválidos para o torneio: {tournament}")
            else:
                logging.warning(f"Dados de torneios ausentes para o país/ID: {sport_tournaments['sport_country_id']}")
    else:
        logging.warning("Resposta dos torneios está vazia ou inválida.")

    # Criar DataFrame
    df_tournaments = pd.DataFrame({
        'tournament_id': list_tournament_id,
        'tournament_name': list_tournament_name,
        'sport_country_id': list_sport_country_id,
        'category_name': list_category_name,
        'updated_at': list_updated_at
    })

    return df_tournaments