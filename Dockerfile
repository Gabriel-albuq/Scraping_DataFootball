# Usa a imagem base do Astro CLI
FROM astrocrpublic.azurecr.io/runtime:3.0-6

# Instala o Poetry
RUN pip install poetry

# Define o diretório de trabalho dentro do contêiner
WORKDIR /usr/local/airflow

# Copia os arquivos de configuração do Poetry
COPY pyproject.toml poetry.lock* ./

# Configura o Poetry para criar o ambiente virtual dentro do projeto
RUN poetry config virtualenvs.in-project true

# Instala as dependências, mas sem o pacote local
RUN poetry install --no-root --only main

# Adiciona o diretório do ambiente virtual do Poetry ao PATH
ENV PATH="/usr/local/airflow/.venv/bin:$PATH"

# --- Adição para resolver o problema de permissão ---

# Cria o diretório de saída para os dados, se ele ainda não existir.
# Isso garante que a pasta 'data/outputs' exista no contêiner.
RUN mkdir -p data/outputs

# Define o dono do diretório para o usuário 'astro', que é o usuário
# padrão do Airflow. Isso concede permissão de escrita.
RUN chown -R astro:astro data

# Copia o restante dos arquivos do projeto
COPY . .