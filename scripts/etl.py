"""
ETAPA 2 — ETL (Extract, Transform, Load) dos dados bancários.

  EXTRACT   -> lê os arquivos brutos de data/raw/
  TRANSFORM -> limpa, padroniza, valida e protege (LGPD) os dados
  LOAD      -> salva os dados tratados em data/processed/

No final, imprime um RELATÓRIO DE QUALIDADE mostrando quantos problemas
foram encontrados e corrigidos em cada tabela.

Como rodar (na pasta do projeto, com o ambiente virtual ativado):
    python scripts/etl.py
"""

import hashlib
import hmac
import os
from pathlib import Path

import pandas as pd

# Chave secreta do hash de CPF. Em produção ela viria de um cofre de segredos
# (variável de ambiente, Vault...), NUNCA escrita no código nem no GitHub.
CHAVE_HASH = os.environ.get("CHAVE_HASH_CPF", "chave-apenas-para-desenvolvimento").encode("utf-8")

PASTA_PROJETO = Path(__file__).resolve().parent.parent
PASTA_RAW = PASTA_PROJETO / "data" / "raw"
PASTA_PROCESSED = PASTA_PROJETO / "data" / "processed"
PASTA_PROCESSED.mkdir(parents=True, exist_ok=True)

# Guarda os números do relatório de qualidade
relatorio = {}


# =====================================================================
# EXTRACT
# =====================================================================

def extrair():
    """Lê os CSVs como TEXTO (dtype=str), para o pandas não 'adivinhar' tipos
    errado. A conversão de cada coluna é feita por nós, de forma controlada."""
    clientes = pd.read_csv(PASTA_RAW / "clientes.csv", dtype=str)
    contas = pd.read_csv(PASTA_RAW / "contas.csv", dtype=str)
    transacoes = pd.read_csv(PASTA_RAW / "transacoes.csv", dtype=str)
    print(f"[EXTRACT] clientes={len(clientes)} | contas={len(contas)} | transacoes={len(transacoes)}")
    return clientes, contas, transacoes


# =====================================================================
# TRANSFORM — funções auxiliares
# =====================================================================

def limpar_cpf(cpf):
    """Remove tudo que não é número: '332.181.960-00' -> '33218196000'."""
    if pd.isna(cpf):
        return None
    return "".join(caractere for caractere in cpf if caractere.isdigit())


def cpf_valido(cpf):
    """Confere os dois dígitos verificadores do CPF (regra da Receita Federal)."""
    if cpf is None or len(cpf) != 11 or cpf == cpf[0] * 11:
        return False
    numeros = [int(d) for d in cpf]
    for posicao in (9, 10):
        soma = sum(numeros[i] * (posicao + 1 - i) for i in range(posicao))
        digito = (soma * 10 % 11) % 10
        if digito != numeros[posicao]:
            return False
    return True


def mascarar_cpf(cpf):
    """LGPD: mostra só o miolo do CPF. '33218196000' -> '***.181.960-**'."""
    if cpf is None or len(cpf) != 11:
        return None
    return f"***.{cpf[3:6]}.{cpf[6:9]}-**"


def gerar_hash_cpf(cpf):
    """LGPD: transforma o CPF num código irreversível (HMAC-SHA256).
    Serve para cruzar dados do mesmo cliente sem expor o CPF real.

    Por que HMAC com chave e não só SHA-256? Existem só ~1 bilhão de CPFs
    possíveis; um atacante poderia calcular o hash de todos e descobrir o CPF.
    Com uma chave secreta, sem a chave isso é impossível."""
    if cpf is None:
        return None
    return hmac.new(CHAVE_HASH, cpf.encode("utf-8"), hashlib.sha256).hexdigest()


def converter_valor(texto):
    """Converte valores nos dois padrões para número (float).
    '1.250,90' (padrão BR) -> 1250.90  |  '1250.90' (padrão US) -> 1250.90"""
    if pd.isna(texto):
        return None
    texto = texto.strip()
    if "," in texto:                       # padrão brasileiro
        texto = texto.replace(".", "").replace(",", ".")
    return float(texto)


def converter_data_hora(serie):
    """Aceita 'AAAA-MM-DD HH:MM:SS' e 'DD/MM/AAAA HH:MM:SS' na mesma coluna."""
    formato_iso = pd.to_datetime(serie, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    formato_br = pd.to_datetime(serie, format="%d/%m/%Y %H:%M:%S", errors="coerce")
    # Onde o formato ISO falhou (NaT), usa o resultado do formato BR
    return formato_iso.fillna(formato_br)


# =====================================================================
# TRANSFORM — uma função por tabela
# =====================================================================

def transformar_clientes(df):
    df = df.copy()

    # 1) CPF: padroniza, valida e protege
    df["cpf_limpo"] = df["cpf"].apply(limpar_cpf)
    df["cpf_valido"] = df["cpf_limpo"].apply(cpf_valido)
    df["cpf_mascarado"] = df["cpf_limpo"].apply(mascarar_cpf)
    df["cpf_hash"] = df["cpf_limpo"].apply(gerar_hash_cpf)

    relatorio["clientes_cpf_com_pontuacao"] = int(df["cpf"].str.contains(r"[.\-]").sum())
    relatorio["clientes_cpf_invalido"] = int((~df["cpf_valido"]).sum())

    # LGPD: o CPF aberto NÃO segue para as próximas etapas
    df = df.drop(columns=["cpf", "cpf_limpo"])

    # 2) Tipos corretos
    df["cliente_id"] = df["cliente_id"].astype(int)
    df["data_nascimento"] = pd.to_datetime(df["data_nascimento"], format="%Y-%m-%d")
    df["renda_mensal"] = pd.to_numeric(df["renda_mensal"], errors="coerce")
    relatorio["clientes_renda_vazia"] = int(df["renda_mensal"].isna().sum())

    # 3) Coluna nova: idade (útil para análise)
    hoje = pd.Timestamp("2026-10-01")  # data de referência fixa = resultado reproduzível
    df["idade"] = ((hoje - df["data_nascimento"]).dt.days // 365).astype(int)

    return df


def transformar_contas(df):
    df = df.copy()
    df["conta_id"] = df["conta_id"].astype(int)
    df["cliente_id"] = df["cliente_id"].astype(int)
    df["data_abertura"] = pd.to_datetime(df["data_abertura"], format="%Y-%m-%d")
    # agencia continua TEXTO: "0042" viraria 42 se convertêssemos para número
    return df


def transformar_transacoes(df, contas):
    df = df.copy()
    total_inicial = len(df)

    # 1) Duplicatas: a mesma transação não pode contar duas vezes
    df = df.drop_duplicates(subset="transacao_id", keep="first")
    relatorio["transacoes_duplicadas_removidas"] = total_inicial - len(df)

    # 2) Tipo: " Pix " / "pix" / "PIX" -> "PIX"
    tipo_original = df["tipo"].copy()
    df["tipo"] = df["tipo"].str.strip().str.upper()
    relatorio["transacoes_tipo_padronizado"] = int((tipo_original != df["tipo"]).sum())

    # 3) Valor: texto -> número
    relatorio["transacoes_valor_formato_br"] = int(df["valor"].str.contains(",").sum())
    df["valor"] = df["valor"].apply(converter_valor)

    # 4) Data/hora: dois formatos -> um só
    relatorio["transacoes_data_formato_br"] = int(df["data_hora"].str.contains("/").sum())
    df["data_hora"] = converter_data_hora(df["data_hora"])

    # 5) Canal vazio -> "desconhecido" (não apagamos a transação: o dinheiro movimentou)
    relatorio["transacoes_canal_vazio"] = int(df["canal"].isna().sum())
    df["canal"] = df["canal"].fillna("desconhecido")

    # 6) Tipos numéricos
    df["transacao_id"] = df["transacao_id"].astype(int)
    df["conta_id"] = df["conta_id"].astype(int)
    df["fraude_confirmada"] = df["fraude_confirmada"].astype(int)

    # 7) Colunas novas
    entradas = ["CREDITO_SALARIO", "PIX_RECEBIDO"]
    df["direcao"] = df["tipo"].apply(lambda t: "entrada" if t in entradas else "saida")
    # valor_assinado: entrada positiva, saída negativa -> somar dá o saldo do período
    df["valor_assinado"] = df.apply(
        lambda linha: linha["valor"] if linha["direcao"] == "entrada" else -linha["valor"], axis=1
    )
    df["data"] = df["data_hora"].dt.date
    df["hora"] = df["data_hora"].dt.hour

    # 8) Integridade: toda transação precisa pertencer a uma conta existente
    sem_conta = ~df["conta_id"].isin(contas["conta_id"])
    relatorio["transacoes_sem_conta"] = int(sem_conta.sum())
    df = df[~sem_conta]

    return df.sort_values("data_hora").reset_index(drop=True)


# =====================================================================
# VALIDAÇÃO — o ETL confere o próprio trabalho antes de salvar
# =====================================================================

def validar(clientes, contas, transacoes):
    assert clientes["cliente_id"].is_unique, "cliente_id repetido!"
    assert contas["conta_id"].is_unique, "conta_id repetido!"
    assert transacoes["transacao_id"].is_unique, "transacao_id repetido!"
    assert transacoes["valor"].notna().all(), "Transação sem valor!"
    assert (transacoes["valor"] > 0).all(), "Transação com valor zero ou negativo!"
    assert transacoes["data_hora"].notna().all(), "Data que não foi convertida!"
    assert "cpf" not in clientes.columns, "CPF aberto vazando para a saída (LGPD)!"
    assert transacoes["hora"].between(0, 23).all(), "Hora fora do intervalo 0-23!"
    print("[VALIDAÇÃO] Todas as regras passaram ✔")


# =====================================================================
# LOAD
# =====================================================================

def carregar(clientes, contas, transacoes):
    clientes.to_csv(PASTA_PROCESSED / "clientes.csv", index=False)
    contas.to_csv(PASTA_PROCESSED / "contas.csv", index=False)
    transacoes.to_csv(PASTA_PROCESSED / "transacoes.csv", index=False)
    print(f"[LOAD] Arquivos tratados salvos em: {PASTA_PROCESSED}")


def imprimir_relatorio():
    print("\n========== RELATÓRIO DE QUALIDADE ==========")
    for problema, quantidade in relatorio.items():
        print(f"  {problema:<36} {quantidade:>6}")
    print("============================================")


# =====================================================================
# EXECUÇÃO
# =====================================================================

if __name__ == "__main__":
    clientes_raw, contas_raw, transacoes_raw = extrair()

    clientes = transformar_clientes(clientes_raw)
    contas = transformar_contas(contas_raw)
    transacoes = transformar_transacoes(transacoes_raw, contas)
    print(f"[TRANSFORM] clientes={len(clientes)} | contas={len(contas)} | transacoes={len(transacoes)}")

    validar(clientes, contas, transacoes)
    carregar(clientes, contas, transacoes)
    imprimir_relatorio()
