-- =====================================================================
-- ETAPA 3A - Modelo de dados no Oracle
--
-- 3 tabelas ligadas por chaves:
--   CLIENTES (1) ---< CONTAS (N) ---< TRANSACOES (N)
--   um cliente tem várias contas, uma conta tem várias transações
--
-- As regras de qualidade do ETL também ficam AQUI, no banco (CHECK).
-- Assim, nem um sistema com erro consegue gravar dado inválido.
-- =====================================================================

-- Apaga as tabelas se já existirem (permite rodar a carga de novo do zero).
-- "IF EXISTS" é novidade do Oracle 23ai. A ordem importa: filhas antes da mãe.
DROP TABLE IF EXISTS transacoes CASCADE CONSTRAINTS PURGE;
DROP TABLE IF EXISTS contas CASCADE CONSTRAINTS PURGE;
DROP TABLE IF EXISTS clientes CASCADE CONSTRAINTS PURGE;


CREATE TABLE clientes (
    cliente_id       NUMBER(10)     CONSTRAINT pk_clientes PRIMARY KEY,
    nome             VARCHAR2(100)  NOT NULL,
    data_nascimento  DATE           NOT NULL,
    cidade           VARCHAR2(60),
    uf               CHAR(2),
    segmento         VARCHAR2(20)   CONSTRAINT ck_clientes_segmento
                                    CHECK (segmento IN ('varejo', 'alta_renda', 'universitario')),
    renda_mensal     NUMBER(12,2),  -- pode ser nula (cliente não informou)
    cpf_valido       NUMBER(1)      CONSTRAINT ck_clientes_cpf_valido CHECK (cpf_valido IN (0, 1)),
    cpf_mascarado    VARCHAR2(14),  -- LGPD: nunca guardamos o CPF aberto
    cpf_hash         VARCHAR2(64),
    idade            NUMBER(3)
);


CREATE TABLE contas (
    conta_id       NUMBER(10)    CONSTRAINT pk_contas PRIMARY KEY,
    cliente_id     NUMBER(10)    NOT NULL
                                 CONSTRAINT fk_contas_clientes REFERENCES clientes (cliente_id),
    agencia        VARCHAR2(4)   NOT NULL,  -- texto, para não perder o zero de "0042"
    numero_conta   VARCHAR2(10)  NOT NULL,
    tipo_conta     VARCHAR2(10)  CONSTRAINT ck_contas_tipo CHECK (tipo_conta IN ('corrente', 'poupanca')),
    data_abertura  DATE          NOT NULL,
    status         VARCHAR2(10)  CONSTRAINT ck_contas_status
                                 CHECK (status IN ('ativa', 'bloqueada', 'encerrada'))
);


CREATE TABLE transacoes (
    transacao_id       NUMBER(12)    CONSTRAINT pk_transacoes PRIMARY KEY,
    conta_id           NUMBER(10)    NOT NULL
                                     CONSTRAINT fk_transacoes_contas REFERENCES contas (conta_id),
    data_hora          TIMESTAMP     NOT NULL,
    tipo               VARCHAR2(20)  NOT NULL
                                     CONSTRAINT ck_transacoes_tipo CHECK (tipo IN
                                     ('PIX', 'TED', 'BOLETO', 'DEBITO', 'CREDITO_SALARIO', 'PIX_RECEBIDO')),
    valor              NUMBER(14,2)  NOT NULL CONSTRAINT ck_transacoes_valor CHECK (valor > 0),
    canal              VARCHAR2(20)  NOT NULL,
    cidade             VARCHAR2(60),
    direcao            VARCHAR2(7)   CONSTRAINT ck_transacoes_direcao CHECK (direcao IN ('entrada', 'saida')),
    valor_assinado     NUMBER(14,2)  NOT NULL,
    hora               NUMBER(2)     CONSTRAINT ck_transacoes_hora CHECK (hora BETWEEN 0 AND 23),
    fraude_confirmada  NUMBER(1)     -- gabarito, só para conferir a análise antifraude
);


-- Índices: aceleram as buscas mais comuns (extrato de uma conta, filtro por período).
-- Sem índice, o Oracle precisa ler a tabela inteira a cada consulta.
CREATE INDEX idx_contas_cliente ON contas (cliente_id);
CREATE INDEX idx_transacoes_conta ON transacoes (conta_id);
CREATE INDEX idx_transacoes_data ON transacoes (data_hora);