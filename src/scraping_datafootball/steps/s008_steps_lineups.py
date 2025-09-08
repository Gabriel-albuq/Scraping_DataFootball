import os
import sys
import pandas as pd
from datetime import datetime, timezone
import logging

from ..scrapers.sofascore_scraper_curl import SofaScoreScraper

def get_lineups(match_id):
    """Busca as escalações de uma partida específica."""
    scraper = SofaScoreScraper()
    url = f"https://www.sofascore.com/api/v1/event/{match_id}/lineups"
    return scraper._make_request(url)

def extract_lineups(match_id):
    """Extrai a resposta do servidor para as escalações"""
    try:
        return [{
            'match_id': match_id,
            'lineups': get_lineups(match_id)
        }]
    except Exception as e:
        logging.error(f"Erro na Match_id: {match_id} - Erro: {str(e)}")
        return None

def transform_lineups(response_matches, datetime_now):
    """Transforma os dados das escalações em um dataframe"""
    data = {
        'match_id_player_id': [], 'match_id': [], 'home_or_away': [], 'formation': [],
        'player_id': [], 'player_name': [], 'player_slug': [], 'list_country': [],
        'list_market_currency': [], 'list_market_value': [], 'list_birthdate': [], 
        'player_position': [], 'player_number': [], 'player_substitute': [],
        'player_captain': [], 'player_out_reason': [], 'player_rating_sofascore': [], 'updated_at': []
    }

    for match in response_matches:
        match_id = match["match_id"]
        if match["lineups"] != None:
            for team_key in ["home", "away"]:
                
                team = match["lineups"].get(team_key, {})
                formation = team.get('formation')
                if team:
                    for player in team.get("players", []):
                        timestamp = player["player"].get("dateOfBirthTimestamp")
                        if isinstance(timestamp, (int, float)) and timestamp > 0:
                            birthdate = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                        else:
                            birthdate = None

                        data['match_id_player_id'].append(f"{match_id}{player['player']['id']}")
                        data['match_id'].append(match_id)
                        data['home_or_away'].append(team_key)
                        data['formation'].append(formation)
                        data['player_id'].append(player["player"]["id"])
                        data['player_name'].append(player["player"].get("name"))
                        data['player_slug'].append(player["player"].get("slug"))
                        data['list_country'].append(player["player"].get("country", {}).get("name"))
                        data['list_market_currency'].append(player["player"].get("proposedMarketValueRaw", {}).get("currency"))
                        data['list_market_value'].append(player["player"].get("proposedMarketValueRaw", {}).get("value"))
                        data['list_birthdate'].append(birthdate)
                        data['player_position'].append(player["player"].get("position"))
                        data['player_number'].append(player["player"].get("jerseyNumber"))
                        data['player_substitute'].append(player.get("substitute"))
                        data['player_captain'].append(player.get("captain"))
                        data['player_out_reason'].append(None)
                        data['player_rating_sofascore'].append(player.get("statistics", {}).get('rating'))
                        data['updated_at'].append(datetime_now)

                    for player in team.get("missingPlayers", []):
                        timestamp = player["player"].get("dateOfBirthTimestamp")
                        if isinstance(timestamp, (int, float)) and timestamp > 0:
                            birthdate = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                        else:
                            birthdate = None

                        data['match_id_player_id'].append(f"{match_id}{player['player']['id']}")
                        data['match_id'].append(match_id)
                        data['home_or_away'].append(team_key)
                        data['formation'].append(formation)
                        data['player_id'].append(player["player"]["id"])
                        data['player_name'].append(player["player"].get("name"))
                        data['player_slug'].append(player["player"].get("slug"))
                        data['list_country'].append(player["player"].get("country", {}).get("name"))
                        data['list_market_currency'].append(player["player"].get("proposedMarketValueRaw", {}).get("currency"))
                        data['list_market_value'].append(player["player"].get("proposedMarketValueRaw", {}).get("value"))
                        data['list_birthdate'].append(birthdate)
                        data['player_position'].append(player["player"].get("position"))
                        data['player_number'].append(player["player"].get("jerseyNumber"))
                        data['player_substitute'].append(player.get("substitute"))
                        data['player_captain'].append(player.get("captain"))
                        data['player_out_reason'].append(None)
                        data['player_rating_sofascore'].append(player.get("statistics", {}).get('rating'))
                        data['updated_at'].append(datetime_now)
        else:
            print(f"Não temos dados de lineups para o match id {match_id}")

    return pd.DataFrame(data)