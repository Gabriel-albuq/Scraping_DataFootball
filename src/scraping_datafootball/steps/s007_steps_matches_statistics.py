import os
import sys
import pandas as pd
import logging
from datetime import datetime, timezone, timedelta

from ..scrapers.sofascore_scraper_curl import SofaScoreScraper

def get_matches_statistics(match_id):
    """
    Busca os dados de uma partida de uma rodada de um torneio específico e temporada.
    """
    url = f"https://www.sofascore.com/api/v1/event/{match_id}/statistics"
    scraper = SofaScoreScraper()
    return scraper._make_request(url)

def extract_matches_statistics(match_id):
    '''
    Extrair a resposta do servidor para as estatísticas

    :param scraper: Classe do SofaScoreScraper
    :return: A resposta do servidor para as estatísticas
    '''
    
    try:
        # Requisita os dados de estatísticas
        response = get_matches_statistics(match_id)
        
        # Verifica se a resposta contém a chave "statistics" válida
        if "statistics" not in response or not (isinstance(response["statistics"], dict) or isinstance(response["statistics"], list)):
            logging.warning(f"Match_id {match_id} não contém estatísticas válidas. Resposta: {response}")
            return None
        
        response_statistics = [{
            'match_id': match_id,
            'statistics': response
        }]
        
    except Exception as e:
        response_statistics = None
        logging.error(f"Erro ao buscar estatísticas para Match_id: {match_id}. Erro: {str(e)}")
    
    return response_statistics

def transform_matches_statistics(response_matches, datetime_now):
    '''
    Pegar os dados de overview das partidas

    :param response_matches: A resposta do servidor ao scraper Matches
    :return: Um dataframe com o overview das partidas
    '''
    list_match_id_key = []
    list_match_id = []
    list_key = []
    list_period = [] 
    list_groupname = [] 
    list_name = [] 
    list_home = [] 
    list_away = [] 
    list_statisticstype = [] 
    list_updated_at = []
    for match in response_matches:
        match_id = match["match_id"]
        
        # Verifica se existe a chave 'statistics' e se está estruturada corretamente
        statistics = match.get("statistics", {}).get("statistics", [])
        if not statistics:
            logging.warning(f"Match_id {match_id} não contém estatísticas válidas.")
            continue

        for stat in statistics:
            period = stat.get("period", "")

            for group in stat.get("groups", []):
                group_name = group.get("groupName", "")

                for item in group.get("statisticsItems", []):
                    name = item.get("name", "")
                    home = item.get("home", 0)
                    away = item.get("away", 0)
                    statistics_type = item.get("statisticsType", "")
                    key = item.get("key", "")

                    list_match_id.append(match_id)
                    list_period.append(period)
                    list_groupname.append(group_name) 
                    list_name.append(name) 
                    list_home.append(home) 
                    list_away.append(away) 
                    list_statisticstype.append(statistics_type) 
                    list_key.append(key)
                    list_match_id_key.append(f"{match_id}{key}")
                    list_updated_at.append(datetime_now)

    df_statistics = pd.DataFrame({
        'match_id_key': list_match_id_key,
        'match_id': list_match_id,
        'period': list_period,
        'groupname': list_groupname,
        'name': list_name,
        'home': list_home,
        'away': list_away,
        'statistics': list_statisticstype,
        'key': list_key,
        'updated_at': list_updated_at,
    })

    return df_statistics