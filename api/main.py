"""
ETAPA 6 — API do Banco Digital (FastAPI).

Junta as 3 fontes de dados do projeto numa API só:
  - Oracle  -> cadastro do cliente e extrato com saldo
  - MongoDB -> eventos do app (logins, troca de celular, limite PIX)
  - Spark   -> alertas de fraude (data/gold/alertas_fraude)

Rodar no seu computador (etapa 6):
    uvicorn api.main:app --reload
Rodar no Docker (etapa 7):
    docker compose up -d --build

Documentação interativa (Swagger): http://localhost:8000/docs
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import oracledb
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from pymongo import MongoClient

PASTA_PROJETO = Path(__file__).resolve().parent.parent
PASTA_ALERTAS = PASTA_PROJETO / "data" / "gold" / "alertas_fraude"

load_dotenv(PASTA_PROJETO / ".env")   # no Docker, as variáveis vêm do docker-compose

# Conexões abertas uma vez, na subida da API, e reaproveitadas em todas as requisições
recursos = {}


@asynccontextmanager
async def ciclo_de_vida(app: FastAPI):
    # POOL de conexões: em vez de abrir uma conexão nova a cada requisição
    # (lento), a API mantém algumas abertas e reaproveita. Padrão em produção.
    recursos["oracle"] = oracledb.create_pool(
        user=os.environ["ORACLE_USER"],
        password=os.environ["ORACLE_USER_PASSWORD"],
        dsn=os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1"),
        min=1, max=4,
    )
    usuario, senha = os.environ["MONGO_USER"], os.environ["MONGO_PASSWORD"]
    host = os.environ.get("MONGO_HOST", "localhost:27017")
    recursos["mongo"] = MongoClient(f"mongodb://{usuario}:{senha}@{host}/?authSource=admin",
                                    serverSelectionTimeoutMS=5000)
    yield
    recursos["oracle"].close()
    recursos["mongo"].close()


app = FastAPI(
    title="API Banco Digital — Antifraude",
    description="Projeto de portfólio: Oracle + MongoDB + PySpark expostos via FastAPI.",
    version="1.0.0",
    lifespan=ciclo_de_vida,
)


# =====================================================================
# MODELOS DE RESPOSTA (Pydantic)
# Definem o formato exato do que a API devolve. O FastAPI usa isso para
# validar a saída e gerar a documentação automática em /docs.
# =====================================================================

class Cliente(BaseModel):
    cliente_id: int
    nome: str
    cpf_mascarado: str | None
    cidade: str | None
    uf: str | None
    segmento: str | None
    idade: int | None
    qtd_contas: int


class Lancamento(BaseModel):
    transacao_id: int
    data_hora: datetime
    tipo: str
    direcao: str
    valor: float
    canal: str
    cidade: str | None
    saldo_periodo: float


class Evento(BaseModel):
    tipo_evento: str
    timestamp: datetime
    detalhes: dict


class Alerta(BaseModel):
    transacao_id: int
    cliente_id: int
    data_hora: datetime
    tipo: str
    valor: float
    media_cliente: float
    z_score: float
    cidade: str
    cidade_cadastro: str
    nota_risco: int


class VisaoRisco(BaseModel):
    cliente_id: int
    alertas_spark: int
    maior_nota_risco: int | None
    trocou_celular: bool
    aumentou_limite_pix: bool
    logins_falhos: int
    nivel_risco: str


# =====================================================================
# FUNÇÕES DE ACESSO AOS DADOS
# =====================================================================

def consultar_oracle(sql, parametros):
    """Executa uma consulta com BIND VARIABLES (:nome) e devolve lista de dicionários."""
    with recursos["oracle"].acquire() as conexao:
        with conexao.cursor() as cursor:
            cursor.execute(sql, parametros)
            colunas = [coluna[0].lower() for coluna in cursor.description]
            return [dict(zip(colunas, linha)) for linha in cursor.fetchall()]


def colecao_eventos():
    return recursos["mongo"]["banco_digital"]["eventos_app"]


def ler_alertas():
    arquivos = sorted(PASTA_ALERTAS.glob("*.csv"))
    if not arquivos:
        raise HTTPException(503, "Alertas ainda não gerados. Rode: docker compose run --rm spark")
    return pd.read_csv(arquivos[0], parse_dates=["data_hora"])


def garantir_cliente_existe(cliente_id):
    if not consultar_oracle("SELECT 1 FROM clientes WHERE cliente_id = :id", {"id": cliente_id}):
        raise HTTPException(404, f"Cliente {cliente_id} não encontrado")


# =====================================================================
# ENDPOINTS
# =====================================================================

@app.get("/health", tags=["Monitoramento"])
def health():
    """Verifica se a API consegue falar com o Oracle e com o MongoDB."""
    status = {}
    try:
        consultar_oracle("SELECT 1 FROM dual", {})
        status["oracle"] = "ok"
    except Exception as erro:
        status["oracle"] = f"erro: {erro}"
    try:
        recursos["mongo"].admin.command("ping")
        status["mongodb"] = "ok"
    except Exception as erro:
        status["mongodb"] = f"erro: {erro}"
    status["alertas_spark"] = "ok" if list(PASTA_ALERTAS.glob("*.csv")) else "não gerados"
    return status


@app.get("/clientes/{cliente_id}", response_model=Cliente, tags=["Clientes"])
def buscar_cliente(cliente_id: int):
    """Cadastro do cliente (Oracle). O CPF sai sempre mascarado (LGPD)."""
    linhas = consultar_oracle("""
        SELECT c.cliente_id, c.nome, c.cpf_mascarado, c.cidade, c.uf,
               c.segmento, c.idade, COUNT(ct.conta_id) AS qtd_contas
          FROM clientes c
          LEFT JOIN contas ct ON ct.cliente_id = c.cliente_id
         WHERE c.cliente_id = :id
         GROUP BY c.cliente_id, c.nome, c.cpf_mascarado, c.cidade, c.uf, c.segmento, c.idade
    """, {"id": cliente_id})
    if not linhas:
        raise HTTPException(404, f"Cliente {cliente_id} não encontrado")
    return linhas[0]


@app.get("/clientes/{cliente_id}/extrato", response_model=list[Lancamento], tags=["Clientes"])
def extrato(cliente_id: int,
            limite: int = Query(20, ge=1, le=500, description="Quantos lançamentos trazer")):
    """Extrato (Oracle), do mais recente para o mais antigo, com o saldo acumulado
    do período calculado por window function, igual à consulta 2 da etapa 3."""
    garantir_cliente_existe(cliente_id)
    return consultar_oracle("""
        SELECT t.transacao_id, t.data_hora, t.tipo, t.direcao, t.valor, t.canal, t.cidade,
               SUM(t.valor_assinado) OVER (ORDER BY t.data_hora, t.transacao_id) AS saldo_periodo
          FROM contas ct
          JOIN transacoes t ON t.conta_id = ct.conta_id
         WHERE ct.cliente_id = :id
         ORDER BY t.data_hora DESC, t.transacao_id DESC
         FETCH FIRST :limite ROWS ONLY
    """, {"id": cliente_id, "limite": limite})


@app.get("/clientes/{cliente_id}/eventos", response_model=list[Evento], tags=["Clientes"])
def eventos(cliente_id: int,
            tipo: str | None = Query(None, description="login, login_falho, troca_dispositivo ou alteracao_limite_pix"),
            limite: int = Query(20, ge=1, le=500)):
    """Eventos do app (MongoDB), do mais recente para o mais antigo."""
    filtro = {"cliente_id": cliente_id}
    if tipo:
        filtro["tipo_evento"] = tipo
    documentos = (colecao_eventos()
                  .find(filtro, {"_id": 0})                 # _id interno do Mongo não sai na API
                  .sort("timestamp", -1)
                  .limit(limite))
    resposta = []
    for doc in documentos:
        tipo_evento = doc.pop("tipo_evento")
        timestamp = doc.pop("timestamp")
        doc.pop("cliente_id")
        resposta.append({"tipo_evento": tipo_evento, "timestamp": timestamp, "detalhes": doc})
    return resposta


@app.get("/alertas-fraude", response_model=list[Alerta], tags=["Antifraude"])
def alertas_fraude(nota_minima: int = Query(70, ge=0, le=100),
                   limite: int = Query(20, ge=1, le=500)):
    """Transações marcadas como suspeitas pelo motor PySpark (etapa 5)."""
    df = ler_alertas()
    df = df[df["nota_risco"] >= nota_minima].head(limite)
    return df.to_dict(orient="records")


@app.get("/clientes/{cliente_id}/risco", response_model=VisaoRisco, tags=["Antifraude"])
def visao_risco(cliente_id: int):
    """Visão 360° de risco: cruza os alertas do Spark com os eventos do MongoDB."""
    garantir_cliente_existe(cliente_id)

    alertas = ler_alertas()
    alertas_cliente = alertas[alertas["cliente_id"] == cliente_id]

    tipos_eventos = colecao_eventos().distinct("tipo_evento", {"cliente_id": cliente_id})
    logins_falhos = colecao_eventos().count_documents(
        {"cliente_id": cliente_id, "tipo_evento": "login_falho"})

    trocou_celular = "troca_dispositivo" in tipos_eventos
    aumentou_limite = "alteracao_limite_pix" in tipos_eventos

    # Regra de negócio simples para o nível de risco
    if len(alertas_cliente) > 0 and trocou_celular and aumentou_limite:
        nivel = "CRÍTICO"
    elif len(alertas_cliente) > 0:
        nivel = "ALTO"
    elif trocou_celular and aumentou_limite:
        nivel = "MÉDIO"
    else:
        nivel = "BAIXO"

    return {
        "cliente_id": cliente_id,
        "alertas_spark": len(alertas_cliente),
        "maior_nota_risco": int(alertas_cliente["nota_risco"].max()) if len(alertas_cliente) else None,
        "trocou_celular": trocou_celular,
        "aumentou_limite_pix": aumentou_limite,
        "logins_falhos": logins_falhos,
        "nivel_risco": nivel,
    }
