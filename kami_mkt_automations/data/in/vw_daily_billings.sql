USE db_uc_kami;
CREATE OR REPLACE VIEW vw_daily_billings AS SELECT  
 IFNULL(CONVERT(YEAR(IFNULL(CASE WHEN nota_fiscal.dt_emissao > 0 THEN nota_fiscal.dt_emissao ELSE nota_fiscal_2.dt_emissao END, pedido.dt_implant)), CHAR), '0') AS ano
,IFNULL(CONVERT(MONTH(IFNULL(CASE WHEN nota_fiscal.dt_emissao > 0 THEN nota_fiscal.dt_emissao ELSE nota_fiscal_2.dt_emissao END, pedido.dt_implant)), CHAR), '0') AS mes
,IFNULL(CONVERT(pedido.cod_empresa, CHAR), '0') AS empresa_pedido
,IFNULL(CONVERT(nota_fiscal_2.cod_empresa, CHAR), '0') AS empresa_nota_fiscal
,IFNULL(CONVERT(pedido.cod_cliente, CHAR), '0') AS cod_cliente
,IFNULL(CONVERT(pedido.cod_colaborador, CHAR), '0') AS cod_colaborador
,IFNULL(CONVERT(colaborador.nome_colaborador, CHAR), '0') AS nome_colaborador
,IFNULL(CONVERT(grupo_colaborador.cod_grupo_venda, CHAR), '0') AS cod_equipe
,IFNULL(CONVERT(grupo_venda.nome_grupo, CHAR), '0') AS equipe
,IFNULL(CONVERT(pedido.cod_pedido, CHAR), '0') AS cod_pedido
,IFNULL(CONVERT(IFNULL(pedido.nr_ped_compra_cli, pedido.cod_pedido_pda), CHAR), '0') AS nr_ped_compra_cli
,IFNULL(CONVERT(pedido.situacao, CHAR), '0') AS cod_situacao
,IFNULL(CONVERT(ponto_controle.descricao, CHAR), '0') AS desc_situacao
,IFNULL(CONVERT(pedido.nop, CHAR), '0') AS nop
,IFNULL(CONVERT(nota_fiscal_2.desc_abrev_cfop, CHAR), '0') AS cfop
,IFNULL(CONVERT(pedido.cod_cond_pagto, CHAR), '0') AS cod_cond_pagto
,IFNULL(CONVERT(pedido_pgto.cod_forma_pagto, CHAR), '0') AS cod_forma_pagto
,IFNULL(CONVERT(forma_pgto.desc_abrev, CHAR), '0') AS forma_pgto
,IFNULL(CONVERT(pedido_item.cod_produto, CHAR), '0') AS cod_produto
,IFNULL(CONVERT(pedido_item.desc_comercial, CHAR), '0') AS desc_produto
,IFNULL(CONVERT(grupo_item.cod_grupo_produto, CHAR), '0') AS cod_grupo_produto
,IFNULL(CONVERT(grupo_produto.desc_abrev, CHAR), '0') AS desc_grupo_produto
,IFNULL(CONVERT(grupo_produto.cod_grupo_pai, CHAR), '0') AS cod_grupo_pai
,IFNULL(CONVERT(grupo_produto_pai.desc_abrev, CHAR), '0') AS desc_grupo_pai
,IFNULL(CONVERT(marca.cod_marca, CHAR), '0') AS cod_marca
,IFNULL(CONVERT(marca.desc_abrev, CHAR), '0') AS desc_marca
,IFNULL(CONVERT(produto_empresa.vl_custo_total, DECIMAL(10,2)), 0.0) AS custo_total
,IFNULL(CONVERT(IFNULL(produto_empresa.vl_custo_kami,(SELECT cpi.preco_unit FROM cd_preco_item AS cpi WHERE cpi.cod_produto = pedido_item.cod_produto AND cpi.tb_preco = 'TabTbCusto')), DECIMAL(10,2)), 0.0) AS custo_kami
,IFNULL(CONVERT(pedido_item.tb_preco, CHAR), '0') AS tb_preco
,IFNULL(CONVERT(pedido_item.qtd, UNSIGNED), 0) AS qtd
,IFNULL(CONVERT(pedido_item.preco_venda, DECIMAL(10,2)), 0.0) AS preco_unit_original
,IFNULL(CONVERT(pedido_item.qtd * pedido_item.preco_venda, DECIMAL(10,2)), 0.0) AS preco_total_original
,IFNULL(CONVERT((((pedido_item.preco_venda / produto_empresa.vl_custo_total)*100)-100), DECIMAL(10,2)), 0.0) AS margem_bruta
,IFNULL(CONVERT(pedido_item.preco_total, DECIMAL(10,2)), 0.0) AS preco_total
,IFNULL(CONVERT((pedido_item.preco_total -( pedido_item.preco_total / pedido.vl_total_produtos) * COALESCE(pedido.vl_desconto,0)), DECIMAL(10,2)), 0.0) AS preco_desconto_rateado
,IFNULL(CONVERT(pedido.vl_total_produtos, DECIMAL(10,2)), 0.0) AS vl_total_pedido
,IFNULL(CONVERT((pedido.vl_desconto * -1), DECIMAL(10,2)), 0.0) AS desconto_pedido
,IFNULL(CONVERT((CASE WHEN nota_fiscal.vl_total_nota_fiscal > 0 THEN nota_fiscal.vl_total_nota_fiscal ELSE nota_fiscal_2.vl_total_nota_fiscal END), DECIMAL(10,2)), 0.0) AS valor_nota
,IFNULL(CONVERT(((CASE WHEN nota_fiscal.vl_total_nota_fiscal > 0 THEN nota_fiscal.vl_total_nota_fiscal ELSE nota_fiscal_2.vl_total_nota_fiscal END) + pedido.vl_desconto), DECIMAL(10,2)), 0.0) AS total_bruto
,IFNULL(CONVERT(pedido.dt_implant, DATETIME), '0000-00-00 00:00:00') AS dt_implante_pedido
,IFNULL(CONVERT(pedido.dt_entrega_comprometida, DATETIME), '0000-00-00 00:00:00') AS dt_entrega_comprometida
,IFNULL(CONVERT((CASE WHEN nota_fiscal.dt_emissao > 0 THEN nota_fiscal.dt_emissao ELSE nota_fiscal_2.dt_emissao END), DATETIME), '0000-00-00 00:00:00') AS dt_faturamento
FROM vd_pedido_item AS pedido_item
LEFT JOIN vd_pedido AS pedido ON (pedido.cod_pedido = pedido_item.cod_pedido AND pedido.cod_empresa = pedido_item.cod_empresa)
LEFT JOIN sg_colaborador AS colaborador ON (colaborador.cod_colaborador = pedido.cod_colaborador)
LEFT JOIN cd_cond_pagto AS cond_pgto ON (cond_pgto.cod_cond_pagto = pedido.cod_cond_pagto)
LEFT JOIN vd_ponto_controle AS ponto_controle ON (ponto_controle.cod_controle = pedido.situacao)
LEFT JOIN vd_pedido_pagto AS pedido_pgto ON (pedido_pgto.cod_pedido = pedido.cod_pedido )
LEFT JOIN cd_forma_pagto AS forma_pgto ON (pedido_pgto.cod_forma_pagto = forma_pgto.cod_forma_pagto)
LEFT JOIN vd_nota_fiscal AS nota_fiscal ON (nota_fiscal.cod_pedido = pedido.cod_pedido AND nota_fiscal.situacao < 86 AND nota_fiscal.situacao > 79 AND pedido.cod_empresa = nota_fiscal.cod_empresa)
LEFT JOIN vd_nota_fiscal AS nota_fiscal_2 ON (nota_fiscal_2.cod_pedido = pedido.cod_pedido AND nota_fiscal_2.situacao < 86 AND nota_fiscal_2.situacao > 79 )
LEFT JOIN cd_produto_empresa AS produto_empresa ON (pedido_item.cod_produto = produto_empresa.cod_produto AND pedido.cod_empresa = produto_empresa.cod_empresa)
LEFT JOIN cd_produto AS produto ON (produto.cod_produto = pedido_item.cod_produto)
LEFT JOIN cd_marca AS marca ON (marca.cod_marca = produto.cod_marca)
LEFT JOIN cd_grupo_item AS grupo_item ON (grupo_item.cod_produto = pedido_item.cod_produto)
LEFT JOIN cd_grupo_produto AS grupo_produto ON (grupo_produto.cod_grupo_produto = grupo_item.cod_grupo_produto)
LEFT JOIN cd_grupo_produto AS grupo_produto_pai ON (grupo_produto_pai.cod_grupo_produto = grupo_produto.cod_grupo_pai)
LEFT JOIN vd_grupo_colaborador AS grupo_colaborador ON (grupo_colaborador.cod_colaborador = pedido.cod_colaborador)
LEFT JOIN vd_grupo AS grupo_venda ON (grupo_venda.cod_grupo_venda = grupo_colaborador.cod_grupo_venda)
WHERE pedido.cod_empresa IN (1,2,3,4,5,6,9,10,11,12,13,14,15,16)
GROUP BY ano, mes, pedido.cod_pedido, pedido.cod_cliente, pedido_item.cod_produto;