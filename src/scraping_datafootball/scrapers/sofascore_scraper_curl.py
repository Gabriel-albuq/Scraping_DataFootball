import os
from curl_cffi import Curl, CurlOpt, CurlError
import json
import gzip
import zlib
import brotli
import time
from dotenv import load_dotenv  # Adicionado
import sys
import logging

class SofaScoreScraper:
    def __init__(self):
        load_dotenv()  # Carrega variáveis do .env
        self.curl = Curl()
        self.origin = 'https://www.sofascore.com/api/v1'
        self.api_key = 'sofascore'
        self.content = None
        self.buffer = bytearray()
        self.response_headers = {}

        # Configurações do proxy a partir do .env
        self.proxy_url = os.getenv("PROXY_URL")
        self.proxy_user = os.getenv("PROXY_USER")
        self.proxy_password = os.getenv("PROXY_PASSWORD")

        # Monta a string de proxy (se todas as variáveis estiverem definidas)
        self.proxy = None
        if self.proxy_url:
            if self.proxy_user and self.proxy_password:
                self.proxy = f"http://{self.proxy_user}:{self.proxy_password}@{self.proxy_url.split('://')[-1]}"
            else:
                self.proxy = self.proxy_url

    def get_headers(self):
        return {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Origin': self.origin,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
        }

    def _handle_response(self, data):
        """ Callback para armazenar os dados recebidos. """
        self.buffer.extend(data)
        return len(data)

    def _handle_header(self, data):
        """ Callback para armazenar cabeçalhos da resposta. """
        header_line = data.decode('utf-8').strip()
        if ":" in header_line:
            key, value = header_line.split(":", 1)
            self.response_headers[key.strip().lower()] = value.strip()
        return len(data)

    def _make_request(self, url):
        headers = self.get_headers()
        max_retries = 6
        attempt = 0

        while attempt < max_retries:
            try:
                self.buffer = bytearray()
                self.response_headers = {}
                self.http_code = None  # Reinicia a cada tentativa

                self.curl.setopt(CurlOpt.URL, url)
                self.curl.setopt(CurlOpt.HTTPHEADER, [f"{k}: {v}".encode('utf-8') for k, v in headers.items()])
                self.curl.setopt(CurlOpt.SSL_VERIFYPEER, 0)
                self.curl.setopt(CurlOpt.SSL_VERIFYHOST, 0)
                self.curl.setopt(CurlOpt.TIMEOUT, 10)
                self.curl.setopt(CurlOpt.FOLLOWLOCATION, 1)
                self.curl.setopt(CurlOpt.MAXREDIRS, 5)
                self.curl.setopt(CurlOpt.WRITEFUNCTION, self._handle_response)
                self.curl.setopt(CurlOpt.HEADERFUNCTION, self._handle_header)

                if self.proxy:
                    self.curl.setopt(CurlOpt.PROXY, self.proxy) # Seta caso tenha proxy
                    logging.info(f"Usando proxy...")

                self.curl.perform()

                if not self.buffer:
                    raise Exception("Nenhum conteúdo recebido.")

                self.http_code = self.curl.getinfo(2097154)  # CurlInfo.RESPONSE_CODE

                if self.http_code == 404:
                    logging.warning(f"Erro 404 - Recurso não encontrado: {url}")
                    return None

                if self.http_code != 200:
                    raise Exception(f"Erro HTTP {self.http_code}: {self.buffer.decode('utf-8', errors='ignore')}")

                content_encoding = self.response_headers.get("content-encoding", "").lower()
                if content_encoding == 'gzip':
                    self.content = gzip.decompress(self.buffer).decode('utf-8')
                elif content_encoding == 'deflate':
                    self.content = zlib.decompress(self.buffer).decode('utf-8')
                elif content_encoding == 'br':
                    self.content = brotli.decompress(self.buffer).decode('utf-8')
                else:
                    self.content = self.buffer.decode('utf-8')

                return json.loads(self.content)

            except Exception as e:
                if self.http_code == 404:
                    logging.warning(f"Erro 404 - Recurso não encontrado. Ignorando futuras tentativas.")
                    return None

                attempt += 1
                logging.warning(f"Tentativa {attempt}/{max_retries} falhou: {e}")
                if attempt < max_retries:
                    time.sleep(3)
                else:
                    raise Exception(f"Falha após {max_retries} tentativas: {e}")