import time
import requests
import os
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

from database import Base, BitcoinPreco

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
    print('Tabela criada/verificada com sucesso!')

def extract_dados_bitcoin():
    '''Extrai o JSON completo da API da Coinbase.'''
    url = 'https://api.coinbase.com/v2/prices/spot'
    resposta = requests.get(url)
    if resposta.status_code == 200:
        return resposta.json()
    else:
        print(f'Erro na API: {resposta.status_code}')
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
    novo_registro = BitcoinPreco(**dados)
    session.add(novo_registro)
    session.commit()
    session.close()
    print(f'[{dados['timestamp']}] Dados salvos no PostgreSQL!')

if __name__ == '__main__':
    create_table()
    print('Iniciando ETL com atualização a cada 15 segundos... (CTRL+C para interromper)')

    while True:
        try:
            dados_json = extract_dados_bitcoin()
            if dados_json:
                dados_tratados = transform_dados_bitcoin(dados_json)
                print('Dados tratados: ', dados_tratados)
                save_data_postgres(dados_tratados)
            time.sleep(15)

        except KeyboardInterrupt:
            print('\nProcesso interrompido pelo usuário. Finalizando...')
            break
        except Exception as e:
            print(f'Erro durante a execução: {e}')
            time.sleep(15)