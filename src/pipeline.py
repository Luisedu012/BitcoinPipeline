import os
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import logging
import logfire
from logging import basicConfig, getLogger

logfire.configure(token="wy4vw22fyv6Mkv957tLsLwb1cg53km4P7GhJ62k5YR6c")
basicConfig(handlers=[logfire.LogfireLoggingHandler()])
logger = getLogger(__name__)
logger.setLevel(logging.INFO)
logfire.instrument_requests()

from database import Base, BitcoinPreco

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


def criar_tabela():
    """Cria a tabela no banco de dados, se não existir."""
    Base.metadata.create_all(engine)
    logger.info("tabela criada/verificada com sucesso!")


def extrair_dados_bitcoin():
    """extrai o Json completo da API da coinbase"""
    url = "https://api.coinbase.com/v2/prices/spot"
    resposta = requests.get(url)
    if resposta.status_code == 200:
        return resposta.json()
    else:
        logger.error(f"Erro na API: {resposta.status_code}")
        return None


def tratar_dados_bitcoin(dados_json):
    """Transforma os dados brutos e adiciona timestanp"""
    valor = float(dados_json["data"]["amount"])
    criptomoeda = dados_json["data"]["base"]
    moeda = dados_json["data"]["currency"]
    timestamp = datetime.now()

    dados_tratados = {
        "valor": valor,
        "criptomoeda": criptomoeda,
        "moeda": moeda,
        "timestamp": timestamp,
    }

    return dados_tratados


def salvar_dados_postgres(dados):
    """salva os dados no banco"""
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


def pipeline_bitcoin():
    """Executa ETL do bitcoin com spans do logfire"""
    with logfire.span("Executando pipeline ETL bitocin"):

        with logfire.span("Extrair dados da API CoinBase"):
            dados_json = extrair_dados_bitcoin()

        if not dados_json:
            logger.error("Falha na extração dos dados. Abortando pipeline")
            return

        with logfire.span("Tratar Dados do Bitcoin"):
            dados_tratados = tratar_dados_bitcoin(dados_json)

        with logfire.span("Salvar Dados Postgres"):
            salvar_dados_postgres(dados_tratados)

        logger.info(f"Pipeline finalizada com sucesso!!!")


if __name__ == "__main__":
    criar_tabela()
    logger.info("Iniciando ETL com atualização a cada 15 segundos...")

    while True:
        try:
            pipeline_bitcoin()
            time.sleep(15)
        except KeyboardInterrupt:
            logger.info("Processo interrompido pelo usuário. Finalizado...")
            break
        except Exception as e:
            logger.error(f"Erro durante a execução: {e}")
            time.sleep(15)
