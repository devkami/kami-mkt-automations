SELECT 
pedido_item.cod_produto,
pedido_item.qtd AS 'qtd',
pedido_item.desc_comercial AS 'desc_comercial',
pedido_item.preco_total AS 'preco_total',
cond_pgto.desc_abrev AS 'cond_pgto',
pedido.cod_cliente AS 'cod_cliente',
pedido.nome_cliente AS 'nome_cliente',
colaborador.cod_colaborador AS 'cod_colaborador',
colaborador.nome_colaborador AS 'nome_colaborador',
colaborador.email AS 'email_colaborador',
pedido.dt_implant AS 'dt_implant',
pedido.nop,
pedido.situacao AS 'cod_situacao',
ponto_controle.descricao AS 'situacao'
FROM vd_pedido_item AS pedido_item
INNER JOIN vd_pedido AS pedido 
  ON (pedido.cod_pedido = pedido_item.cod_pedido
  AND pedido.nop IN ("6.102","6.404","BLACKFRIDAY","VENDA","VENDA_S_ESTOQUE","WORKSHOP", "BONIFICADO")
  AND pedido.situacao >= 20
  AND pedido.situacao < 200)
INNER JOIN cd_cond_pagto AS cond_pgto ON cond_pgto.cod_cond_pagto = pedido.cod_cond_pagto
INNER JOIN sg_colaborador AS colaborador ON colaborador.cod_colaborador = pedido.cod_colaborador
INNER JOIN vd_ponto_controle AS ponto_controle ON ponto_controle.cod_controle = pedido.situacao
WHERE pedido_item.cod_produto IN (
    '13711-13',
    '13711-16',
    '13711-17',
    '13711-18',
    'BR13711-13',
    'BR13711-16',
    'BR13711-17',
    'BR13711-18'
);