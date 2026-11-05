"""
ETAPA 3A — Carga dos dados tratados no Oracle.

  1. Conecta no Oracle (que está rodando no Docker)
  2. Cria as tabelas executando sql/01_criar_tabelas.sql
  3. Insere clientes, contas e transações de data/processed/ (em lotes)
  4. Confere se a quantidade de linhas no banco bate com os arquivos

Antes de rodar:
  - Oracle no ar:  docker compose up -d   (e esperar ficar "healthy")
  - ETL já rodado: python scripts/etl.py

Como rodar (na pasta do projeto, com o venv ativado):
    python scripts/carregar_oracle.py
"""

import os
import re
import time
from pathlib import Path

import oracledb
import pandas as pd
from dotenv import load_dotenv

PASTA_PROJETO = Path(__file__).resolve().parent.parent
PASTA_PROCESSED = PASTA_PROJETO / "data" / "processed"
ARQUIVO_DDL = PASTA_PROJETO / "sql" / "01_criar_tabelas.sql"

# Lê usuário, senha e endereço do banco do arquivo .env (nada de senha no código!)
load_dotenv(PASTA_PROJETO / ".env")
USUARIO = os.environ["ORACLE_USER"]
SENHA = os.environ["ORACLE_USER_PASSWORD"]
DSN = os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1")

TAMANHO_LOTE = 5000  # quantas linhas enviar ao banco por vez


# =====================================================================
# CONEXÃO
# =====================================================================

def conectar(tentativas=5, espera_segundos=10):
    """Conecta no Oracle. Se o banco ainda estiver iniciando, tenta de novo."""
    for tentativa in range(1, tentativas + 1):
        try:
            conexao = oracledb.connect(user=USUARIO, password=SENHA, dsn=DSN)
            print(f"[CONEXÃO] Conectado ao Oracle {conexao.version} em {DSN} ✔")
            return conexao
        except oracledb.DatabaseError as erro:
            print(f"[CONEXÃO] Tentativa {tentativa}/{tentativas} falhou: {erro}")
            if tentativa == tentativas:
                raise
            time.sleep(espera_segundos)


# =====================================================================
# CRIAÇÃO DAS TABELAS
# =====================================================================

def ler_comandos_sql(caminho):
    """Lê um arquivo .sql e separa cada comando (eles terminam com ';').
    O driver do Oracle executa UM comando por vez, por isso a separação."""
    texto = caminho.read_text(encoding="utf-8")
    texto = re.sub(r"--.*$", "", texto, flags=re.MULTILINE)   # remove comentários
    return [comando.strip() for comando in texto.split(";") if comando.strip()]


def criar_tabelas(cursor):
    comandos = ler_comandos_sql(ARQUIVO_DDL)
    for comando in comandos:
        cursor.execute(comando)
    print(f"[DDL] {len(comandos)} comandos executados (tabelas e índices criados) ✔")


# =====================================================================
# CARGA
# =====================================================================

def preparar_linhas(df):
    """Converte o DataFrame em lista de tuplas, que é o formato que o driver aceita.
    Valores vazios (NaN/NaT do pandas) viram None, que no Oracle vira NULL."""
    df = df.astype(object).where(df.notna(), None)
    return list(df.itertuples(index=False, name=None))


def inserir(cursor, tabela, df):
    colunas = list(df.columns)
    # Monta: INSERT INTO tabela (col1, col2) VALUES (:1, :2)
    # Os ":1, :2" são "bind variables": o valor vai separado do comando.
    # Isso é mais rápido e PROTEGE CONTRA SQL INJECTION.
    marcadores = ", ".join(f":{i}" for i in range(1, len(colunas) + 1))
    sql = f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({marcadores})"

    # Diz ao driver o tipo de cada coluna. Sem isso, ele "adivinha" pela
    # primeira linha, e um valor vazio ali (ex.: renda nula) quebraria a carga.
    tipos = []
    for coluna in colunas:
        if pd.api.types.is_datetime64_any_dtype(df[coluna]):
            tipos.append(oracledb.DB_TYPE_DATE)
        elif pd.api.types.is_numeric_dtype(df[coluna]):
            tipos.append(oracledb.DB_TYPE_NUMBER)
        else:
            tipos.append(int(df[coluna].astype(str).str.len().max()))  # texto: tamanho máximo
    cursor.setinputsizes(*tipos)

    linhas = preparar_linhas(df)
    for inicio in range(0, len(linhas), TAMANHO_LOTE):
        lote = linhas[inicio:inicio + TAMANHO_LOTE]
        cursor.executemany(sql, lote)   # envia o lote inteiro de uma vez
    print(f"[LOAD] {tabela:<11} {len(linhas):>6} linhas inseridas")


def ler_processados():
    clientes = pd.read_csv(PASTA_PROCESSED / "clientes.csv", parse_dates=["data_nascimento"])
    clientes["cpf_valido"] = clientes["cpf_valido"].astype(int)   # True/False -> 1/0

    contas = pd.read_csv(PASTA_PROCESSED / "contas.csv", parse_dates=["data_abertura"],
                         dtype={"agencia": str})                   # mantém o "0042"

    transacoes = pd.read_csv(PASTA_PROCESSED / "transacoes.csv", parse_dates=["data_hora"])
    transacoes = transacoes.drop(columns=["data"])  # o Oracle tira a data de data_hora

    return clientes, contas, transacoes


# =====================================================================
# CONFERÊNCIA
# =====================================================================

def conferir(cursor, esperado):
    """Compara a contagem de linhas no banco com a dos arquivos."""
    print("\n========== CONFERÊNCIA ==========")
    tudo_ok = True
    for tabela, qtd_arquivo in esperado.items():
        cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
        qtd_banco = cursor.fetchone()[0]
        status = "✔" if qtd_banco == qtd_arquivo else "✘ DIVERGENTE"
        tudo_ok = tudo_ok and qtd_banco == qtd_arquivo
        print(f"  {tabela:<11} arquivo={qtd_arquivo:>6} | banco={qtd_banco:>6}  {status}")
    print("=================================")
    return tudo_ok


def amostra_por_tipo(cursor):
    """Uma primeira consulta SQL, para ver os dados vivos no banco."""
    cursor.execute("""
        SELECT tipo,
               COUNT(*)            AS quantidade,
               ROUND(SUM(valor),2) AS volume_total
          FROM transacoes
         GROUP BY tipo
         ORDER BY volume_total DESC
    """)
    print("\nVolume por tipo de transação:")
    for tipo, quantidade, volume in cursor.fetchall():
        print(f"  {tipo:<16} {quantidade:>6} transações   R$ {volume:>15,.2f}")


#
# =====================================================================
# EXECUÇÃO
# =====================================================================

if __name__ == "__main__":
    clientes, contas, transacoes = ler_processados()

    with conectar() as conexao:
        with conexao.cursor() as cursor:
            criar_tabelas(cursor)

            # Ordem obrigatória por causa das chaves estrangeiras:
            # não existe conta sem cliente, nem transação sem conta.
            inserir(cursor, "clientes", clientes)
            inserir(cursor, "contas", contas)
            inserir(cursor, "transacoes", transacoes)

            ok = conferir(cursor, {"clientes": len(clientes),
                                   "contas": len(contas),
                                   "transacoes": len(transacoes)})
            if ok:
                conexao.commit()      # grava de vez
                print("[COMMIT] Dados gravados no Oracle ✔")
                amostra_por_tipo(cursor)
            else:
                conexao.rollback()    # desfaz tudo: ou carrega certo, ou não carrega nada
                print("[ROLLBACK] Divergência encontrada, carga desfeita ✘")