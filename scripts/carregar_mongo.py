"""
ETAPA 4 — Eventos do app no MongoDB.

Por que MongoDB e não Oracle? Cada tipo de evento tem campos DIFERENTES:
  - login_falho           -> tem "motivo"
  - troca_dispositivo     -> tem "dispositivo_anterior"
  - alteracao_limite_pix  -> tem "limite_anterior" e "limite_novo"
  - alguns têm "geolocalizacao", outros não
No Oracle, isso viraria uma tabela cheia de colunas vazias. No MongoDB,
cada documento guarda só os campos que tem (esquema flexível).

O que o script faz:
  1. Conecta no MongoDB (rodando no Docker)
  2. Converte o timestamp de texto para data (mini-ETL)
  3. Insere os 8.000 eventos e cria índices
  4. Confere a contagem
  5. Roda 3 análises (aggregation pipelines), incluindo um alerta de
     possível invasão de conta (account takeover)

Como rodar (na pasta do projeto, com o venv ativado e o Docker no ar):
    python scripts/carregar_mongo.py
"""

import json
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient

PASTA_PROJETO = Path(__file__).resolve().parent.parent
ARQUIVO_EVENTOS = PASTA_PROJETO / "data" / "raw" / "eventos_app.json"

load_dotenv(PASTA_PROJETO / ".env")
USUARIO = os.environ["MONGO_USER"]
SENHA = os.environ["MONGO_PASSWORD"]
HOST = os.environ.get("MONGO_HOST", "localhost:27017")

NOME_BANCO = "banco_digital"
NOME_COLECAO = "eventos_app"


# =====================================================================
# CONEXÃO
# =====================================================================

def conectar():
    uri = f"mongodb://{USUARIO}:{SENHA}@{HOST}/?authSource=admin"
    cliente = MongoClient(uri, serverSelectionTimeoutMS=10000)
    cliente.admin.command("ping")            # falha aqui se o Mongo não estiver no ar
    versao = cliente.server_info()["version"]
    print(f"[CONEXÃO] Conectado ao MongoDB {versao} em {HOST} ✔")
    return cliente


# =====================================================================
# EXTRACT + TRANSFORM
# =====================================================================

def ler_eventos():
    with open(ARQUIVO_EVENTOS, encoding="utf-8") as arquivo:
        eventos = json.load(arquivo)

    # Mini-ETL: o JSON guarda a data como TEXTO ("2026-07-01T10:30:00").
    # Convertendo para datetime, o MongoDB grava como data de verdade e
    # conseguimos filtrar por período, ordenar e agrupar por dia.
    for evento in eventos:
        evento["timestamp"] = datetime.fromisoformat(evento["timestamp"])

    print(f"[EXTRACT] {len(eventos)} eventos lidos de {ARQUIVO_EVENTOS.name}")
    return eventos


# =====================================================================
# LOAD
# =====================================================================

def carregar(colecao, eventos):
    colecao.drop()                     # recomeça do zero a cada execução
    resultado = colecao.insert_many(eventos)
    print(f"[LOAD] {len(resultado.inserted_ids)} documentos inseridos em {NOME_COLECAO}")

    # Índices: buscar os eventos de um cliente por período é a consulta mais comum
    colecao.create_index([("cliente_id", ASCENDING), ("timestamp", ASCENDING)])
    colecao.create_index([("tipo_evento", ASCENDING)])
    print("[ÍNDICES] cliente_id+timestamp e tipo_evento criados ✔")


def conferir(colecao, esperado):
    no_banco = colecao.count_documents({})
    status = "✔" if no_banco == esperado else "✘ DIVERGENTE"
    print(f"\n[CONFERÊNCIA] arquivo={esperado} | mongo={no_banco}  {status}")
    return no_banco == esperado


# =====================================================================
# ANÁLISES (aggregation pipeline = o "GROUP BY" do MongoDB)
# =====================================================================

def eventos_por_tipo(colecao):
    pipeline = [
        {"$group": {"_id": "$tipo_evento", "quantidade": {"$sum": 1}}},
        {"$sort": {"quantidade": -1}},
    ]
    print("\n1) Eventos por tipo:")
    for linha in colecao.aggregate(pipeline):
        print(f"   {linha['_id']:<22} {linha['quantidade']:>5}")


def motivos_login_falho(colecao):
    pipeline = [
        {"$match": {"tipo_evento": "login_falho"}},                 # = WHERE
        {"$group": {"_id": "$motivo", "quantidade": {"$sum": 1}}},  # = GROUP BY
        {"$sort": {"quantidade": -1}},                              # = ORDER BY
    ]
    print("\n2) Motivos de login falho:")
    for linha in colecao.aggregate(pipeline):
        print(f"   {linha['_id']:<22} {linha['quantidade']:>5}")


def alerta_account_takeover(colecao):
    """Padrão clássico de invasão de conta: o golpista troca o celular
    cadastrado E aumenta o limite do PIX para conseguir sacar mais.
    Buscamos clientes que tiveram os DOIS eventos."""
    pipeline = [
        {"$match": {"tipo_evento": {"$in": ["troca_dispositivo", "alteracao_limite_pix"]}}},
        {"$group": {
            "_id": "$cliente_id",
            "tipos": {"$addToSet": "$tipo_evento"},     # lista sem repetição
            "maior_limite_novo": {"$max": "$limite_novo"},
            "ultimo_evento": {"$max": "$timestamp"},
        }},
        {"$match": {"tipos": {"$all": ["troca_dispositivo", "alteracao_limite_pix"]}}},
        {"$sort": {"maior_limite_novo": -1, "_id": 1}},
    ]
    alertas = list(colecao.aggregate(pipeline))
    print(f"\n3) ALERTA: {len(alertas)} clientes trocaram de celular E aumentaram o limite PIX")
    print("   (mostrando os 10 com maior limite novo)")
    for linha in alertas[:10]:
        data = linha["ultimo_evento"].strftime("%d/%m/%Y")
        print(f"   cliente {linha['_id']:>3} | limite novo R$ {linha['maior_limite_novo']:>6,} | último evento {data}")


def esquema_flexivel(colecao):
    """Mostra na prática a vantagem do MongoDB: documentos com campos diferentes."""
    com_geo = colecao.count_documents({"geolocalizacao": {"$exists": True}})
    sem_geo = colecao.count_documents({"geolocalizacao": {"$exists": False}})
    print(f"\n4) Esquema flexível: {com_geo} eventos COM geolocalização, {sem_geo} SEM")


# =====================================================================
# EXECUÇÃO
# =====================================================================

if __name__ == "__main__":
    eventos = ler_eventos()
    total = len(eventos)

    cliente = conectar()
    colecao = cliente[NOME_BANCO][NOME_COLECAO]

    carregar(colecao, eventos)
    if conferir(colecao, total):
        eventos_por_tipo(colecao)
        motivos_login_falho(colecao)
        alerta_account_takeover(colecao)
        esquema_flexivel(colecao)

    cliente.close()
