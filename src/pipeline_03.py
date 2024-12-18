import time
import requests
import os
import logging
import logfire
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from logging import basicConfig, getLogger

from database import Base, BitcoinPreco

# ------------------------------------------------------------------
# Configuração Logfire
logfire.configure()
basicConfig(handlers=[logfire.LogfireLoggingHandler()])
logger = getLogger(__name__)
logger.setLevel(logging.INFO)
logfire.instrument_requests()
logfire.instrument_sqlalchemy()

# ------------------------------------------------------------------



# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Lê as variáveis separadas do arquivo .env (sem SSL)
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')

DATABASE_URL = (
    f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}'
    f'@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
)

# Cria a engine e a sessão
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def create_table():
    '''Cria a tabela no banco de dados, se não existir'''
    Base.metadata.create_all(engine)
    logger.info('Tabela criada/verificada com sucesso!')

def extract_dados_bitcoin():
    '''Extrai o JSON completo da API da Coinbase.'''
    url = 'https://api.coinbase.com/v2/prices/spot'
    resposta = requests.get(url)
    if resposta.status_code == 200:
        return resposta.json()
    else:
        logger.info(f'Erro na API: {resposta.status_code}')
        return None

def transform_dados_bitcoin(dados):
    valor = dados['data']['amount']
    cripto = dados['data']['base']
    moeda = dados['data']['currency']
    timestamp = datetime.now()

    dados_transformados = {
        'valor': valor,
        'criptomoeda': cripto,
        'moeda': moeda,
        'timestamp': timestamp
    }

    return dados_transformados

def save_data_postgres(dados):
    '''Salva os dados no banco PostgreSQL'''
    session = Session()
    try:
        novo_registro = BitcoinPreco(**dados)
        session.add(novo_registro)
        session.commit()
        logger.info(f"[{dados['timestamp']}] Dados salvos no PostgreSQL!")
    except Exception as ex:
        logger.error(f"Erro ao inserir dados no PostgreSQL: {ex}")
        session.rollback()
    finally:
        session.close()

if __name__ == '__main__':
    create_table()
    logger.info('Iniciando ETL com atualização a cada 15 segundos... (CTRL+C para interromper)')

    while True:
        try:
            dados_json = extract_dados_bitcoin()
            if dados_json:
                dados_tratados = transform_dados_bitcoin(dados_json)
                logger.info('Dados tratados: ', dados_tratados)
                save_data_postgres(dados_tratados)
            time.sleep(15)

        except KeyboardInterrupt:
            logger.info('\nProcesso interrompido pelo usuário. Finalizando...')
            break
        except Exception as e:
            logger.info(f'Erro durante a execução: {e}')
            time.sleep(15)