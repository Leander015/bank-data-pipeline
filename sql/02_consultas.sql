   -- Consulta 1: Extrato de um cliente (JOIN de 3 tabelas)

SELECT c.nome,
        t.data_hora,
        t.tipo,
        t.direcao,
        t.valor,
        t.canal
    FROM clientes c
    JOIN contas   ct ON ct.cliente_id = c.cliente_id
    JOIN transacoes t ON t.conta_id = ct.conta_id
    WHERE c.cliente_id = 150
    ORDER BY t.data_hora;

-- Consulta 2: Extrato com saldo acumulado (window function)
SELECT t.data_hora,
       t.tipo,
       t.valor_assinado,
       SUM(t.valor_assinado) OVER (ORDER BY t.data_hora, t.transacao_id) AS saldo_acumulado
  FROM contas     ct
  JOIN transacoes t ON t.conta_id = ct.conta_id
 WHERE ct.cliente_id = 150
 ORDER BY t.data_hora, t.transacao_id;

 -- Consulta 3: Top 10 clientes por volume movimentado (GROUP BY + RANK)
SELECT *
  FROM (
        SELECT c.cliente_id,
               c.nome,
               c.segmento,
               COUNT(*)      AS qtd_transacoes,
               SUM(t.valor)  AS volume_movimentado,
               RANK() OVER (ORDER BY SUM(t.valor) DESC) AS posicao
          FROM clientes   c
          JOIN contas     ct ON ct.cliente_id = c.cliente_id
          JOIN transacoes t  ON t.conta_id    = ct.conta_id
         GROUP BY c.cliente_id, c.nome, c.segmento
       )
 WHERE posicao <= 10
 ORDER BY posicao;

 -- Consulta 4: Top 5 clientes de alta renda
 SELECT *
    FROM (
            SELECT c.cliente_id,
                 c.nome,
                 c.segmento,
                 COUNT(*)      AS qtd_transacoes,
                 SUM(t.valor)  AS volume_movimentado,
                 RANK() OVER (ORDER BY SUM(t.valor) DESC) AS posicao
            FROM clientes   c
            JOIN contas     ct ON ct.cliente_id = c.cliente_id
            JOIN transacoes t  ON t.conta_id    = ct.conta_id
             WHERE c.segmento = 'alta_renda'
             GROUP BY c.cliente_id, c.nome, c.segmento
         )
     WHERE posicao <= 5
     ORDER BY posicao;

-- Consulta 5: PIX suspeitos - madrugada e 5x acima da média do cliente (CTE)
WITH media_cliente AS (
    SELECT ct.cliente_id,
           AVG(t.valor) AS valor_medio
      FROM contas     ct
      JOIN transacoes t ON t.conta_id = ct.conta_id
     GROUP BY ct.cliente_id
)
SELECT ct.cliente_id,
       t.data_hora,
       t.valor,
       ROUND(m.valor_medio, 2)           AS media_do_cliente,
       ROUND(t.valor / m.valor_medio, 1) AS vezes_acima_media,
       t.fraude_confirmada
  FROM transacoes    t
  JOIN contas        ct ON ct.conta_id  = t.conta_id
  JOIN media_cliente m  ON m.cliente_id = ct.cliente_id
 WHERE t.tipo = 'PIX'
   AND t.hora BETWEEN 0 AND 5
   AND t.valor > 5 * m.valor_medio
 ORDER BY vezes_acima_media DESC;