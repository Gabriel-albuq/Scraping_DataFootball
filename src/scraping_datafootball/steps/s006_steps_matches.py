import os
import sys
import pandas as pd
from datetime import datetime, timezone, timedelta
import logging

from ..scrapers.sofascore_scraper_curl import SofaScoreScraper
from ..utils.save_response_json import save_response_to_json, save_response_json_to_s3
from ..utils.save_dataframe_csv import save_dataframe_to_csv

def get_matches(unique_tournament_id, season_id, round, slug=None):
    """
    Busca os dados de uma partida de uma rodada de um torneio específico e temporada.
    """
    scraper = SofaScoreScraper()
    if slug:
        url = f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{round}/slug/{slug}"
    else:
        url = f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{round}"
    return scraper._make_request(url)

def extract_matches(tournament_id, season_id, round, slug):
    '''
    Extrair a resposta do servidor ao scraper das Partidas

    :param scraper: Classe do SofaScoreScraper
    :return: A resposta do servidor ao scraper das Partidas
    '''
    response_matches = [{
        'unique_tournament_id': tournament_id,
        'season_id': season_id,
        'round': round,
        'slug': slug,
        'matches': get_matches(tournament_id, season_id, round, slug)
    }]

    return response_matches

def transform_matches(response_matches, datetime_now):
    '''
    Transformar os dados do response_matches em um dataframe

    :param response_matches: A resposta do servidor ao scraper matches
    :return: Um dataframe com as partidas
    '''
    list_season_id_round_slug = []
    list_unique_tournament_id = []
    list_season_id = []
    list_round = []
    list_slug = []
    list_tournament_name = []
    list_match_id = []
    list_match_slug = []
    list_match_timestamp = []
    list_match_datetime = []
    list_home_team_id = []
    list_home_team_name = []
    list_home_score = []
    list_away_team_id = []
    list_away_team_name = []
    list_away_score = []
    list_cup_round_type = []
    list_updated_at = []
    
    for match in response_matches:
        unique_tournament_id = match["unique_tournament_id"]
        season_id = match["season_id"]
        round = match['round']
        slug = match['slug']
        
        for match_data in match['matches']['events']:
            tournament_name = match_data['tournament']['name']
            match_id = match_data['id']
            match_slug = match_data['slug']
            match_timestamp = int(match_data['startTimestamp'])
            match_datetime_utc = datetime.fromtimestamp(match_timestamp, tz=timezone.utc)
            match_datetime_br = match_datetime_utc.astimezone(timezone(timedelta(hours=-3)))
            home_team_id = match_data['homeTeam']['id']
            home_team_name = match_data['homeTeam']['name']
            away_team_id = match_data['awayTeam']['id']
            away_team_name = match_data['awayTeam']['name']

            try:
                home_score = match_data['homeScore']['current']
            except:
                home_score = None

            try:
                away_score = match_data['awayScore']['current']
            except:
                away_score = None

            try:
                cup_round_type = str(match_data['roundInfo']['cupRoundType'])
            except:
                cup_round_type = None
            
            list_unique_tournament_id.append(unique_tournament_id)
            list_season_id.append(season_id)
            list_round.append(round)
            list_slug.append(slug)
            list_tournament_name.append(tournament_name)
            list_match_id.append(match_id)
            list_match_slug.append(match_slug)
            list_match_timestamp.append(match_timestamp) # Não está pegando corretamente
            list_match_datetime.append(match_datetime_br) # Não está pegando corretamente
            list_home_team_id.append(home_team_id)
            list_home_team_name.append(home_team_name)
            list_home_score.append(home_score)
            list_away_team_id.append(away_team_id)
            list_away_team_name.append(away_team_name)
            list_away_score.append(away_score)
            list_cup_round_type.append(cup_round_type)
            list_updated_at.append(datetime_now)

            if slug == None:
                list_season_id_round_slug.append(f"{season_id}{round}")
            else:
                list_season_id_round_slug.append(f"{season_id}{round}{slug}")

    # Criar DataFrame
    df_matches = pd.DataFrame({
        'season_id_round_slug': list_season_id_round_slug,
        'unique_tournament_id': list_unique_tournament_id,
        'season_id': list_season_id,
        'round': list_round,
        'slug': list_slug,
        'tournament_name': list_tournament_name,
        'match_id': list_match_id,
        'cup_round_type': list_cup_round_type,
        'match_slug': list_match_slug,
        'match_timestamp': list_match_timestamp,
        'match_datatime': list_match_datetime,
        'home_team_id': list_home_team_id,
        'home_team_name': list_home_team_name,
        'home_score': list_home_score,
        'away_score': list_away_score,
        'away_team_name': list_away_team_name,
        'away_team_id': list_away_team_id,
        'updated_at': list_updated_at,
    })

    return df_matches
