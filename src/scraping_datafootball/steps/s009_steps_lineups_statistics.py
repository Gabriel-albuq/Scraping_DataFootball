import os
import sys
import pandas as pd
import logging
from datetime import datetime, timezone, timedelta

from ..scrapers.sofascore_scraper_curl import SofaScoreScraper

def get_lineups_statistics(match_id):
    """
    Busca as escalações de uma partida específica.
    """
    scraper = SofaScoreScraper()
    url = f"https://www.sofascore.com/api/v1/event/{match_id}/lineups"
    return scraper._make_request(url)

def extract_lineups_statistics(match_id):
    '''
    Extrair a resposta do servidor para as escalações

    :param match_id: ID da partida
    :return: A resposta do servidor para as escalações
    '''
    try:
        extract_lineups_statistics = [{
            'match_id': match_id,
            'lineups': get_lineups_statistics(match_id)
        }]
    except Exception as e:
        extract_lineups_statistics = None
        logging.error(f"Erro na Match_id: {match_id} - Erro: {e}")
        pass

    return extract_lineups_statistics

def transform_lineups_statistics(response_matches, datetime_now):
    '''
    Pegar os dados de overview das partidas

    :param response_matches: A resposta do servidor ao scraper Matches
    :return: Um dataframe com o overview das partidas
    '''
    list_match_id_player_id_statistic_name = []
    list_match_id_player_id = []
    list_match_id = []
    list_home_or_away = [] 
    list_formation = []
    list_player_id = []
    list_player_name = []
    list_player_slug = []
    list_player_position = []
    list_player_number = []
    list_player_substitute = []
    list_player_captain = []
    list_player_out_reason = [] # 1: Machucado / 2: Suspenso
    list_player_country = []
    list_player_market_currency = []
    list_player_market_value = [] 
    list_player_brithdate = []
    list_player_statistic_name = []   
    list_player_statistic_value = []
    list_updated_at = []

    for match in response_matches:
        match_id = match["match_id"]

        for team_key in ["home", "away"]:
            team = match["lineups"].get(team_key, {})
            formation = team['formation']
            if team:  # Verifica se o time existe (não está vazio)
                # Relacionados
                for player in team.get("players", []):
                    home_or_away = (team_key)  # "home" ou "away"
                    formation = (formation)
                    player_id = (player["player"]["id"])
                    player_out_reason = (None)
                    try:
                        player_market_currency = (player["player"]["proposedMarketValueRaw"]["currency"])
                        player_market_value = (player["player"]["proposedMarketValueRaw"]["value"])
                    except:
                        player_market_currency = (None)
                        player_market_value = (None)

                    try:
                        player_position = player["player"]["position"]
                    except:
                        player_position = None

                    try:
                        player_name = player["player"]["name"]
                    except:
                        player_name = None

                    try:
                        player_slug = player["player"]["slug"]
                    except:
                        player_slug = None

                    try:
                        player_country = player["player"]["country"]["name"]
                    except:
                        player_country = None

                    try:
                        brithdate = datetime.fromtimestamp(player["player"]["dateOfBirthTimestamp"], tz=timezone.utc)
                        player_brithdate = brithdate
                    except:
                        player_brithdate = None

                    try:
                        player_number = (player["player"]["jerseyNumber"])
                    except:
                        player_number = (None)

                    try:
                        player_substitute = (player["substitute"])
                    except:
                        player_substitute = (None)

                    try:
                        player_captain = (player["captain"]) # Apenas os capitães tem esse campo
                    except:
                        player_captain = (None)

                    try:
                        for stat_name, stat_value in player["statistics"].items():
                            list_match_id_player_id_statistic_name.append(f"{match_id}{player['player']['id']}{stat_name}")
                            list_match_id_player_id.append(f"{match_id}{player['player']['id']}")
                            list_match_id.append(match_id)
                            list_home_or_away.append(home_or_away)  # "home" ou "away"
                            list_formation.append(formation)
                            list_player_id.append(player_id)
                            list_player_name.append(player_name)
                            list_player_slug.append(player_slug)
                            list_player_market_currency.append(player_market_currency)
                            list_player_market_value.append(player_market_value)
                            list_player_position.append(player_position)
                            list_player_number.append(player_number)
                            list_player_country.append(player_country)
                            list_player_brithdate.append(player_brithdate)
                            list_player_substitute.append(player_substitute)
                            list_player_captain.append(player_captain)
                            list_player_out_reason.append(player_out_reason) 
                            list_player_statistic_name.append(stat_name)     
                            list_player_statistic_value.append(stat_value)
                            list_updated_at.append(datetime_now)
                    except:
                        list_match_id_player_id_statistic_name.append(f"{match_id}{player['player']['id']}{stat_name}")
                        list_match_id_player_id.append(f"{match_id}{player['player']['id']}")
                        list_match_id.append(match_id)
                        list_home_or_away.append(home_or_away)  # "home" ou "away"
                        list_formation.append(formation)
                        list_player_id.append(player_id)
                        list_player_name.append(player_name)
                        list_player_slug.append(player_slug)
                        list_player_market_currency.append(player_market_currency)
                        list_player_market_value.append(player_market_value)
                        list_player_position.append(player_position)
                        list_player_number.append(player_number)
                        list_player_country.append(player_country)
                        list_player_brithdate.append(player_brithdate)
                        list_player_substitute.append(player_substitute)
                        list_player_captain.append(player_captain)
                        list_player_out_reason.append(player_out_reason) 
                        list_player_statistic_name.append(None)     
                        list_player_statistic_value.append(None)
                        list_updated_at.append(datetime_now)            

    df_lineups_statistics = pd.DataFrame({
        "match_id_player_id_statistic_name": list_match_id_player_id_statistic_name,
        "match_id_player_id": list_match_id_player_id,
        "match_id": list_match_id,
        "home_or_away": list_home_or_away,
        "formation": list_formation,
        "player_id": list_player_id,
        "player_name": list_player_name,
        "player_slug": list_player_slug,
        "list_player_country": list_player_country,
        "list_player_market_currency": list_player_market_currency,
        "list_player_market_value": list_player_market_value,
        "list_player_brithdate": list_player_brithdate,
        "player_position": list_player_position,
        "player_number": list_player_number,
        "player_substitute": list_player_substitute,
        "player_captain": list_player_captain,
        "player_out_reason": list_player_out_reason,
        "player_statistic_name": list_player_statistic_name,
        "player_statistic_value": list_player_statistic_value,
        'updated_at': list_updated_at,
    })

    return df_lineups_statistics