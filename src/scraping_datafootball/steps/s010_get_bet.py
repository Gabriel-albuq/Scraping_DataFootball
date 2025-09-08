import os
import sys
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta

from ..scrapers.sofascore_scraper_curl import SofaScoreScraper

def get_bets():
    """
    Busca os dados dos esportes disponíveis.
    """
    url = f"https://oddspedia.com/api/v1/getMatchOdds?wettsteuer=0&geoCode=BR&bookmakerGeoCode=BR&bookmakerGeoState=&matchKey=3765&oddGroupId=1&inplay=0&language=br"
    scraper = SofaScoreScraper()
    
    response_content = scraper._make_request(url)
    
    # Se a resposta não for nula e for binária, decodifique-a para string
    if response_content and isinstance(response_content, bytes):
        try:
            # Tente decodificar para UTF-8. Se não funcionar, tente outras codificações.
            decoded_response = response_content.decode('utf-8')
            return decoded_response
        except UnicodeDecodeError:
            logging.error("Erro ao decodificar a resposta binária.")
            return None
            
    return response_content

def extract_bets():
    '''
    Extrair a resposta do servidor ao scraper dos Esportes

    :param scraper: Classe do SofaScoreScraper
    :return: A resposta do servidor ao scraper bets
    '''
    try:
        response_bets = get_bets()
        if not isinstance(response_bets, dict):
            logging.error(f"Erro na resposta dos esportes: {response_bets}")
            return None
    except Exception as e:
        logging.exception(f"Erro ao buscar dados dos esportes: {str(e)}")
        response_bets = None
    
    return response_bets

if __name__ == "__main__":
    response_bets = get_bets()
    print(response_bets)