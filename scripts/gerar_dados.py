"""
ETAPA 1 — Gerador de dados bancários fictícios.

Cria 4 arquivos em data/raw/:
  - clientes.csv      -> cadastro de clientes (vai para o Oracle)
  - contas.csv        -> contas correntes/poupança (vai para o Oracle)
  - transacoes.csv    -> PIX, TED, boleto, débito, crédito (Oracle + PySpark)
  - eventos_app.json  -> logins e acessos ao app, formato variável (MongoDB)

Os dados vêm "sujos" de propósito, como numa extração real, para a etapa
de ETL ter o que tratar:
  - CPFs com e sem pontuação, alguns inválidos
  - tipo de transação escrito de jeitos diferentes ("PIX", "pix", " Pix ")
  - valores com vírgula decimal ("1.250,90")
  - datas em dois formatos (2026-08-01 e 01/08/2026)
  - transações duplicadas e campos vazios

Também existem algumas transações suspeitas (valores muito acima do padrão
do cliente, de madrugada, em outra cidade), para a análise antifraude.

Como rodar (na pasta do projeto):
    python scripts/gerar_dados.py
"""

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)  # mesma semente = mesmos dados sempre

PASTA_SAIDA = Path(__file__).resolve().parent.parent / "data" / "raw"
PASTA_SAIDA.mkdir(parents=True, exist_ok=True)

QTD_CLIENTES = 500
DATA_INICIAL = datetime(2026, 7, 1)
QTD_DIAS = 90

NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Felipe", "Gabriela", "Hugo",
         "Isabela", "João", "Karen", "Lucas", "Mariana", "Nathan", "Olívia", "Pedro"]
SOBRENOMES = ["Silva", "Souza", "Oliveira", "Costa", "Pereira", "Lima", "Alves",
              "Ribeiro", "Carvalho", "Gomes"]
CIDADES = [("Brasília", "DF"), ("Goiânia", "GO"), ("São Paulo", "SP"),
           ("Belo Horizonte", "MG"), ("Rio de Janeiro", "RJ"), ("Curitiba", "PR")]
SEGMENTOS = ["varejo", "varejo", "varejo", "alta_renda", "universitario"]

# Tipo de transação: (nome, faixa de valor mínimo, máximo, é saída de dinheiro?)
TIPOS_TRANSACAO = [
    ("PIX", 5, 1500, True),
    ("TED", 200, 8000, True),
    ("BOLETO", 50, 2500, True),
    ("DEBITO", 5, 400, True),
    ("CREDITO_SALARIO", 1500, 12000, False),
    ("PIX_RECEBIDO", 10, 2000, False),
]
CANAIS = ["app", "app", "app", "internet_banking", "agencia", "caixa_eletronico"]


# ---------- funções auxiliares ----------

def gerar_cpf():
    """Gera um CPF fictício com dígitos verificadores válidos."""
    numeros = [random.randint(0, 9) for _ in range(9)]
    for peso_inicial in (10, 11):
        soma = sum(n * p for n, p in zip(numeros, range(peso_inicial, 1, -1)))
        digito = (soma * 10 % 11) % 10
        numeros.append(digito)
    return "".join(str(n) for n in numeros)


def sujar_cpf(cpf):
    """Às vezes formata com pontuação, às vezes estraga um dígito."""
    sorteio = random.random()
    if sorteio < 0.40:
        return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"   # 123.456.789-09
    if sorteio < 0.43:
        return cpf[:-1] + str((int(cpf[-1]) + 1) % 10)        # CPF inválido
    return cpf


def sujar_tipo(tipo):
    sorteio = random.random()
    if sorteio < 0.10:
        return tipo.lower()
    if sorteio < 0.15:
        return f" {tipo.title()} "
    return tipo


def formatar_valor(valor):
    """~20% dos valores vêm no padrão brasileiro: 1.250,90"""
    if random.random() < 0.20:
        texto = f"{valor:,.2f}"                     # 1,250.90
        return texto.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{valor:.2f}"


def formatar_data(dt):
    """~25% das datas vêm em formato brasileiro."""
    if random.random() < 0.25:
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ---------- geração das tabelas ----------

def gerar_clientes():
    clientes = []
    for i in range(1, QTD_CLIENTES + 1):
        cidade, uf = random.choice(CIDADES)
        nascimento = datetime(1960, 1, 1) + timedelta(days=random.randint(0, 16000))
        clientes.append({
            "cliente_id": i,
            "nome": f"{random.choice(NOMES)} {random.choice(SOBRENOMES)}",
            "cpf": sujar_cpf(gerar_cpf()),
            "data_nascimento": nascimento.strftime("%Y-%m-%d"),
            "cidade": cidade,
            "uf": uf,
            "segmento": random.choice(SEGMENTOS),
            # renda vazia em ~5% dos clientes
            "renda_mensal": "" if random.random() < 0.05 else round(random.uniform(1500, 25000), 2),
        })
    return clientes


def gerar_contas(clientes):
    contas = []
    conta_id = 1
    for cliente in clientes:
        tipos = ["corrente"]
        if random.random() < 0.30:
            tipos.append("poupanca")      # 30% também têm poupança
        for tipo in tipos:
            abertura = DATA_INICIAL - timedelta(days=random.randint(30, 3000))
            contas.append({
                "conta_id": conta_id,
                "cliente_id": cliente["cliente_id"],
                "agencia": random.choice(["0001", "0042", "1234"]),
                "numero_conta": f"{random.randint(10000, 99999)}-{random.randint(0, 9)}",
                "tipo_conta": tipo,
                "data_abertura": abertura.strftime("%Y-%m-%d"),
                "status": random.choices(["ativa", "bloqueada", "encerrada"], [0.93, 0.04, 0.03])[0],
            })
            conta_id += 1
    return contas


def gerar_transacoes(contas, clientes):
    cidade_do_cliente = {c["cliente_id"]: c["cidade"] for c in clientes}
    correntes = [c for c in contas if c["tipo_conta"] == "corrente"]

    transacoes = []
    transacao_id = 1
    for conta in correntes:
        cidade_casa = cidade_do_cliente[conta["cliente_id"]]
        qtd = random.randint(30, 150)                     # transações no período
        for _ in range(qtd):
            tipo, vmin, vmax, _saida = random.choice(TIPOS_TRANSACAO)
            momento = DATA_INICIAL + timedelta(
                days=random.randint(0, QTD_DIAS - 1),
                hours=random.randint(7, 22),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )
            valor = round(random.uniform(vmin, vmax), 2)
            cidade = cidade_casa if random.random() < 0.95 else random.choice(CIDADES)[0]
            suspeita = False

            # ~0,5% de transações suspeitas: valor alto, madrugada, outra cidade
            if random.random() < 0.005:
                tipo = "PIX"
                valor = round(random.uniform(8000, 45000), 2)
                momento = momento.replace(hour=random.randint(1, 4))
                cidade = random.choice([c for c, _ in CIDADES if c != cidade_casa])
                suspeita = True

            transacoes.append({
                "transacao_id": transacao_id,
                "conta_id": conta["conta_id"],
                "data_hora": formatar_data(momento),
                "tipo": sujar_tipo(tipo),
                "valor": formatar_valor(valor),
                "canal": "" if random.random() < 0.02 else random.choice(CANAIS),
                "cidade": cidade,
                # gabarito: NÃO usar no modelo, só para conferir a análise no final
                "fraude_confirmada": int(suspeita),
            })
            transacao_id += 1

    # ~1% de duplicadas (erro de reprocessamento)
    duplicadas = random.sample(transacoes, k=int(len(transacoes) * 0.01))
    transacoes += duplicadas
    random.shuffle(transacoes)
    return transacoes


def gerar_eventos_app(clientes):
    """Eventos semi-estruturados: cada tipo tem campos diferentes (bom para MongoDB)."""
    eventos = []
    for _ in range(8000):
        cliente = random.choice(clientes)
        momento = DATA_INICIAL + timedelta(minutes=random.randint(0, QTD_DIAS * 24 * 60))
        tipo = random.choices(["login", "login_falho", "troca_dispositivo", "alteracao_limite_pix"],
                              [0.80, 0.12, 0.04, 0.04])[0]
        evento = {
            "cliente_id": cliente["cliente_id"],
            "tipo_evento": tipo,
            "timestamp": momento.isoformat(),
            "dispositivo": {
                "so": random.choice(["Android", "iOS"]),
                "versao_app": random.choice(["5.2.0", "5.3.1", "5.4.0"]),
            },
        }
        if tipo == "login_falho":
            evento["motivo"] = random.choice(["senha_incorreta", "biometria_falhou", "token_expirado"])
        if tipo == "troca_dispositivo":
            evento["dispositivo_anterior"] = random.choice(["Android", "iOS"])
        if tipo == "alteracao_limite_pix":
            evento["limite_anterior"] = random.choice([1000, 2000, 5000])
            evento["limite_novo"] = random.choice([10000, 20000, 50000])
        if random.random() < 0.6:
            evento["geolocalizacao"] = {"cidade": cliente["cidade"], "uf": cliente["uf"]}
        eventos.append(evento)
    return eventos


def salvar_csv(linhas, nome_arquivo):
    with open(PASTA_SAIDA / nome_arquivo, "w", newline="", encoding="utf-8") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=linhas[0].keys())
        escritor.writeheader()
        escritor.writerows(linhas)
    print(f"{nome_arquivo:<18} -> {len(linhas):>6} linhas")


def salvar_json(dados, nome_arquivo):
    with open(PASTA_SAIDA / nome_arquivo, "w", encoding="utf-8") as arquivo:
        json.dump(dados, arquivo, ensure_ascii=False, indent=2)
    print(f"{nome_arquivo:<18} -> {len(dados):>6} documentos")


if __name__ == "__main__":
    clientes = gerar_clientes()
    contas = gerar_contas(clientes)
    transacoes = gerar_transacoes(contas, clientes)
    eventos = gerar_eventos_app(clientes)

    salvar_csv(clientes, "clientes.csv")
    salvar_csv(contas, "contas.csv")
    salvar_csv(transacoes, "transacoes.csv")
    salvar_json(eventos, "eventos_app.json")
    print(f"\nArquivos salvos em: {PASTA_SAIDA}")
