USE db_uc_kami;
CREATE OR REPLACE VIEW vw_monthly_billings AS
SELECT 
 IFNULL(CONVERT(YEAR(pd.dt_implant), CHAR), '0') AS ano
,IFNULL(CONVERT(MONTH(pd.dt_implant), CHAR), '0') AS mes
,IFNULL(CONVERT(pd.cod_empresa, CHAR), '0') AS cod_empresa
,IFNULL(CONVERT(pd.cod_pedido, CHAR), '0') AS cod_pedido
,IFNULL(CONVERT(pd.cod_cliente, CHAR), '0') AS cod_cliente
,IFNULL(CONVERT(pd.nome_cliente, CHAR), '0') AS nome_cliente
,IFNULL(CONVERT(IFNULL(pd.nr_ped_compra_cli, pd.cod_pedido_pda), CHAR), '0') AS nr_ped_compra_cli
,IFNULL(CONVERT(pd.situacao, CHAR), '0') AS situacao_pedido
,IFNULL(CONVERT(pd.nop, CHAR), '0') AS nop
,IFNULL(CONVERT(nf2.desc_abrev_cfop, CHAR), '0') AS desc_abrev_cfop
,IFNULL(CONVERT(ra.desc_abrev, CHAR), '0') AS desc_abreviada
,IFNULL(CONVERT(pd.cod_colaborador, CHAR), '0') AS cod_colaborador
,IFNULL(CONVERT(sc.nome_colaborador, CHAR), '0') AS nome_colaborador
,IFNULL(CONVERT(pd.cod_cond_pagto, CHAR), '0') AS cod_cond_pagto
,IFNULL(CONVERT(pp.cod_forma_pagto, CHAR), '0') AS cod_forma_pagto
,IFNULL(CONVERT(fp.desc_abrev, CHAR), '0') AS desc_abrev
,IFNULL(CONVERT(pdi.cod_produto, CHAR), '0') AS cod_produto
,IFNULL(CONVERT(pdi.desc_comercial, CHAR), '0') AS desc_comercial
,IFNULL(CONVERT(ROUND(pdi.qtd,0), UNSIGNED),'0') AS qtd
,IFNULL(CONVERT(ce.vl_custo_total, DECIMAL(10,2)), 0.0) AS custo_total
,IFNULL(CONVERT(IFNULL(ce.vl_custo_kami, (SELECT cpi.preco_unit FROM cd_preco_item AS cpi WHERE cpi.cod_produto = pdi.cod_produto AND cpi.tb_preco = 'TabTbCusto')), DECIMAL(10,2)), 0.0) AS custo_kami
,IFNULL(CONVERT(pdi.tb_preco, CHAR), '0') AS tb_preco
,IFNULL(CONVERT(pdi.preco_venda, DECIMAL(10,2)), 0.0) AS preco_unit_original
,IFNULL(CONVERT((pdi.qtd * pdi.preco_venda), DECIMAL(10,2)), 0.0) AS preco_total_original
,IFNULL(CONVERT((((pdi.preco_venda / ce.vl_custo_total)*100)-100), DECIMAL(10,2)), 0.0) AS margem_bruta
,IFNULL(CONVERT(pdi.preco_total, DECIMAL(10,2)), 0.0) AS preco_total
,IFNULL(CONVERT((pdi.preco_total -( pdi.preco_total / pd.vl_total_produtos) * COALESCE(pd.vl_desconto,0)), DECIMAL(10,2)), 0.0) AS preco_desconto_rateado
,IFNULL(CONVERT(pd.vl_total_produtos, DECIMAL(10,2)), 0.0) AS vl_total_pedido
,IFNULL(CONVERT((pd.vl_desconto * -1), DECIMAL(10,2)), 0.0) AS  desconto_pedido
,IFNULL(CONVERT(CASE WHEN nf.vl_total_nota_fiscal > 0 then nf.vl_total_nota_fiscal ELSE nf2.vl_total_nota_fiscal END, DECIMAL(10,2)), 0.0) AS valor_nota
,IFNULL(CONVERT(pd.dt_implant, DATETIME), '0000-00-00 00:00:00') AS  dt_implante_pedido
,IFNULL(CONVERT(pd.dt_entrega_comprometida, DATETIME), '0000-00-00 00:00:00') AS dt_entrega_comprometida
,IFNULL(CONVERT(pd.situacao, CHAR), '0') AS situacao_entrega
,IFNULL(CONVERT(vp.descricao, CHAR), '0') AS descricao
,IFNULL(CONVERT((CASE WHEN nf.dt_emissao > 0 then nf.dt_emissao ELSE nf2.dt_emissao END), DATETIME), '0000-00-00 00:00:00') AS dt_faturamento
,IFNULL(CONVERT(marca.desc_abrev, CHAR), '0') AS marca
,IFNULL(CONVERT(pd.cod_empresa, CHAR), '0') AS empresa_pedido
,IFNULL(CONVERT(nf2.cod_empresa, CHAR), '0') AS empresa_nf
FROM vd_pedido AS pd
LEFT JOIN sg_colaborador AS sc ON (sc.cod_colaborador = pd.cod_colaborador)
LEFT JOIN cd_cond_pagto AS cp ON  (cp.cod_cond_pagto = pd.cod_cond_pagto)
LEFT JOIN vd_ponto_controle AS vp ON (vp.cod_controle = pd.situacao)
LEFT JOIN vd_pedido_pagto AS pp ON (pp.cod_pedido = pd.cod_pedido )
LEFT JOIN cd_forma_pagto AS fp ON  (pp.cod_forma_pagto = fp.cod_forma_pagto)
LEFT JOIN cd_cliente_atividade AS ca  ON (ca.cod_cliente = pd.cod_cliente)
LEFT JOIN cd_ramo_atividade AS ra  ON (ca.cod_ramo_atividade = ra.cod_ramo_atividade)
LEFT JOIN vd_nota_fiscal AS nf ON (nf.cod_pedido = pd.cod_pedido AND nf.situacao < 86 AND nf.situacao > 79 AND  pd.cod_empresa = nf.cod_empresa)
LEFT JOIN vd_nota_fiscal AS nf2 ON (nf2.cod_pedido = pd.cod_pedido AND nf2.situacao < 86 AND nf2.situacao > 79)
LEFT JOIN vd_pedido_item AS pdi ON (pd.cod_pedido = pdi.cod_pedido AND pd.cod_empresa = pdi.cod_empresa)
LEFT JOIN cd_produto_empresa AS ce ON (pdi.cod_produto = ce.cod_produto AND pd.cod_empresa = ce.cod_empresa)
LEFT JOIN cd_produto AS produto ON (produto.cod_produto = pdi.cod_produto)
LEFT JOIN cd_marca AS marca ON (marca.cod_marca = produto.cod_marca)
WHERE pd.dt_implant >= "2023-01-01"
AND pd.cod_empresa in (1,2,3,4,5,6,9,10,11,12,13,14,15,16)
AND pd.situacao < 200
GROUP BY ano, mes, pd.cod_pedido, pd.cod_cliente, pdi.cod_produto;
