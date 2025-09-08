import os
import sys
import pandas as pd
from datetime import datetime, timezone, timedelta
import logging

from ..scrapers.sofascore_scraper_curl import SofaScoreScraper

def get_seasons(tournament_id):
    """
    Busca os dados das seasons disponíveis no torneio.
    """
    scraper = SofaScoreScraper()
    url = f"https://www.sofascore.com/api/v1/unique-tournament/{tournament_id}/seasons"
    return scraper._make_request(url)


def extract_seasons(tournament_id):
    """
    Extrair a resposta do servidor ao scraper das Temporadas

    :param scraper: Classe do SofaScoreScraper
    :return: A resposta do servidor ao scraper das Temporadas
    """
    response_seasons = [{
        'unique_tournament_id': tournament_id,
        'seasons': get_seasons(tournament_id)
    }]

    return response_seasons


def transform_seasons(response_seasons, datetime_now):
    """
    Transformar os dados do response_seasons em um dataframe

    :param response_seasons: A resposta do servidor ao scraper seasons
    :return: Um dataframe com as temporadas
    """
    list_unique_tournament_id = []
    list_tournament_season_name = []
    list_season_year = []
    list_season_id = []
    list_updated_at = []

    for tournament_id in response_seasons:
        if tournament_id['seasons'] != None:
            for season in tournament_id['seasons']['seasons']:
                list_unique_tournament_id.append(tournament_id['unique_tournament_id'])
                list_tournament_season_name.append(season['name'])
                list_season_year.append(season['year'])
                list_season_id.append(season['id'])
                list_updated_at.append(datetime_now)

    # Criar DataFrame
    df_seasons = pd.DataFrame({
        'unique_tournament_id': list_unique_tournament_id,
        'tournament_season_name': list_tournament_season_name,
        'season_year': list_season_year,
        'season_id': list_season_id,
        'updated_at': list_updated_at
    })

    return df_seasons