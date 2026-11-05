"""
ETAPA 5 — Motor antifraude com PySpark.

Na etapa 3, uma regra em SQL pegou 86% das fraudes. Aqui o PySpark calcula
o PERFIL de cada cliente e dá uma NOTA DE RISCO (0 a 100) para cada
transação, combinando vários sinais. É assim que bancos processam milhões
de transações: o Spark divide o trabalho entre vários núcleos (ou máquinas).

  1. Lê os dados tratados (data/processed)
  2. Monta o perfil de cada cliente com WINDOW FUNCTIONS (média, desvio padrão)
  3. Calcula sinais de risco e a nota de cada transação
  4. Avalia o modelo contra o gabarito (precisão e recall)
  5. Salva os alertas em data/gold/alertas_fraude (lido pela API na etapa 6)

Como rodar (o Spark roda dentro do Docker, não precisa instalar Java):
    docker compose run --rm spark
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)

PASTA_PROCESSED = "data/processed"
PASTA_SAIDA = "data/gold/alertas_fraude"
NOTA_MINIMA_ALERTA = 70

# Pesos de cada sinal na nota de risco (somam 100)
PESO_MADRUGADA = 40          # transação entre 0h e 5h
PESO_VALOR_ATIPICO = 30      # valor muito acima do padrão do cliente
PESO_CIDADE_DIFERENTE = 20   # fora da cidade cadastrada
PESO_PIX = 10                # PIX cai na hora: o meio preferido dos golpes


def criar_sessao():
    spark = (SparkSession.builder
             .appName("antifraude-banco")
             .config("spark.sql.session.timeZone", "America/Sao_Paulo")
             # O padrão do Spark é dividir em 200 partes (pensado para big data).
             # Para 45 mil linhas, 8 partes é mais rápido: menos "overhead".
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.ui.showConsoleProgress", "false")   # sem barras de progresso
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")   # esconde os logs internos do Spark
    print(f"[SPARK] Sessão iniciada — Spark {spark.version} ✔")
    return spark


# =====================================================================
# 1. LEITURA
# =====================================================================

# Esquema explícito: mais rápido (o Spark não precisa "adivinhar" lendo o arquivo
# duas vezes) e mais seguro (garante o tipo certo de cada coluna).
ESQUEMA_TRANSACOES = StructType([
    StructField("transacao_id", IntegerType()),
    StructField("conta_id", IntegerType()),
    StructField("data_hora", TimestampType()),
    StructField("tipo", StringType()),
    StructField("valor", DoubleType()),
    StructField("canal", StringType()),
    StructField("cidade", StringType()),
    StructField("fraude_confirmada", IntegerType()),
    StructField("direcao", StringType()),
    StructField("valor_assinado", DoubleType()),
    StructField("data", StringType()),
    StructField("hora", IntegerType()),
])


def ler_dados(spark):
    transacoes = spark.read.csv(f"{PASTA_PROCESSED}/transacoes.csv",
                                header=True, schema=ESQUEMA_TRANSACOES)
    contas = (spark.read.csv(f"{PASTA_PROCESSED}/contas.csv", header=True, inferSchema=True)
              .select("conta_id", "cliente_id"))
    clientes = (spark.read.csv(f"{PASTA_PROCESSED}/clientes.csv", header=True, inferSchema=True)
                .select("cliente_id", F.col("cidade").alias("cidade_cadastro"), "segmento"))

    # Junta tudo: cada transação ganha o cliente dono e a cidade dele
    df = (transacoes
          .join(contas, "conta_id")
          .join(clientes, "cliente_id"))
    print(f"[LEITURA] {df.count()} transações carregadas")
    return df


# =====================================================================
# 2. PERFIL DO CLIENTE (window functions)
# =====================================================================

def calcular_perfil(df):
    # A "janela" agrupa as transações de cada cliente SEM juntar as linhas:
    # cada transação continua existindo e ganha a média/desvio do seu dono.
    # É a mesma ideia do SUM(...) OVER (...) que você fez no Oracle.
    janela_cliente = Window.partitionBy("cliente_id")

    return (df
            .withColumn("media_cliente", F.avg("valor").over(janela_cliente))
            .withColumn("desvio_cliente", F.stddev("valor").over(janela_cliente))
            # z-score: quantos "desvios padrão" a transação está acima da média.
            # Acima de 3 é estatisticamente raro (menos de 0,3% dos casos normais).
            .withColumn("z_score",
                        (F.col("valor") - F.col("media_cliente")) / F.col("desvio_cliente")))


# =====================================================================
# 3. SINAIS DE RISCO E NOTA
# =====================================================================

def calcular_nota(df):
    df = (df
          .withColumn("sinal_madrugada", F.col("hora").between(0, 5))
          .withColumn("sinal_valor_atipico", F.col("z_score") > 3)
          .withColumn("sinal_cidade_diferente", F.col("cidade") != F.col("cidade_cadastro"))
          .withColumn("sinal_pix", F.col("tipo") == "PIX"))

    # Cada sinal verdadeiro soma seus pontos na nota (when = "se", otherwise = "senão")
    nota = (F.when(F.col("sinal_madrugada"), PESO_MADRUGADA).otherwise(0)
            + F.when(F.col("sinal_valor_atipico"), PESO_VALOR_ATIPICO).otherwise(0)
            + F.when(F.col("sinal_cidade_diferente"), PESO_CIDADE_DIFERENTE).otherwise(0)
            + F.when(F.col("sinal_pix"), PESO_PIX).otherwise(0))

    return (df
            .withColumn("nota_risco", nota)
            .withColumn("alerta", F.col("nota_risco") >= NOTA_MINIMA_ALERTA))


# =====================================================================
# 4. AVALIAÇÃO CONTRA O GABARITO
# =====================================================================

def avaliar(df):
    resumo = df.groupBy("alerta", "fraude_confirmada").count().collect()
    contagem = {(linha["alerta"], linha["fraude_confirmada"]): linha["count"] for linha in resumo}

    vp = contagem.get((True, 1), 0)    # verdadeiro positivo: alertou e ERA fraude
    fp = contagem.get((True, 0), 0)    # falso positivo: alertou mas NÃO era (cliente incomodado)
    fn = contagem.get((False, 1), 0)   # falso negativo: NÃO alertou e era fraude (prejuízo!)
    vn = contagem.get((False, 0), 0)   # verdadeiro negativo: transação normal, sem alerta

    precisao = vp / (vp + fp) if vp + fp else 0
    recall = vp / (vp + fn) if vp + fn else 0

    print("\n=============== AVALIAÇÃO DO MODELO ===============")
    print(f"  Alertas gerados:                  {vp + fp:>6}")
    print(f"  Fraudes reais no período:         {vp + fn:>6}")
    print("  ---------------------------------------------------")
    print(f"  Fraudes detectadas (VP):          {vp:>6}")
    print(f"  Alarmes falsos (FP):              {fp:>6}")
    print(f"  Fraudes que escaparam (FN):       {fn:>6}")
    print(f"  Transações normais ok (VN):       {vn:>6}")
    print("  ---------------------------------------------------")
    print(f"  PRECISÃO: {precisao:6.1%}  (dos alertas, quantos eram fraude)")
    print(f"  RECALL:   {recall:6.1%}  (das fraudes, quantas foram pegas)")
    print("  Regra SQL da etapa 3 tinha recall de 86%")
    print("===================================================")


def mostrar_destaques(df):
    print("\nSinais mais presentes nas transações em alerta:")
    (df.filter("alerta")
       .agg(F.round(F.avg(F.col("sinal_madrugada").cast("int")) * 100, 1).alias("% madrugada"),
            F.round(F.avg(F.col("sinal_valor_atipico").cast("int")) * 100, 1).alias("% valor atipico"),
            F.round(F.avg(F.col("sinal_cidade_diferente").cast("int")) * 100, 1).alias("% outra cidade"),
            F.round(F.avg(F.col("sinal_pix").cast("int")) * 100, 1).alias("% PIX"))
       .show())

    print("Top 5 alertas com maior nota de risco:")
    (df.filter("alerta")
       .select("transacao_id", "cliente_id", "data_hora", "tipo",
               F.round("valor", 2).alias("valor"),
               F.round("z_score", 1).alias("z_score"),
               "cidade", "cidade_cadastro", "nota_risco")
       .orderBy(F.desc("nota_risco"), F.desc("z_score"))
       .show(5, truncate=False))


# =====================================================================
# 5. SAÍDA
# =====================================================================

def salvar_alertas(df):
    alertas = (df.filter("alerta")
               .select("transacao_id", "cliente_id", "conta_id", "data_hora", "tipo",
                       F.round("valor", 2).alias("valor"),
                       F.round("media_cliente", 2).alias("media_cliente"),
                       F.round("z_score", 2).alias("z_score"),
                       "cidade", "cidade_cadastro", "nota_risco")
               .orderBy(F.desc("nota_risco"), F.desc("z_score")))

    # coalesce(1): junta tudo num arquivo só (o Spark normalmente gera vários,
    # um por partição). Para poucos dados, um arquivo é mais prático.
    alertas.coalesce(1).write.mode("overwrite").csv(PASTA_SAIDA, header=True)
    print(f"[SAÍDA] {alertas.count()} alertas salvos em {PASTA_SAIDA}/ ✔")


# =====================================================================
# EXECUÇÃO
# =====================================================================

if __name__ == "__main__":
    spark = criar_sessao()

    dados = ler_dados(spark)
    dados = calcular_perfil(dados)
    dados = calcular_nota(dados)
    dados.cache()   # guarda em memória: vamos usar o resultado várias vezes

    avaliar(dados)
    mostrar_destaques(dados)
    salvar_alertas(dados)

    spark.stop()
