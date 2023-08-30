CREATE OR REPLACE VIEW vw_future_bills AS
SELECT recebe.cod_empresa AS empresa
,recebe.dt_vencimento AS dt_vencimento
,(sum(recebe.vl_total_titulo) - sum(recebe.vl_total_baixa)) AS total_a_receber
FROM fn_titulo_receber AS recebe 
WHERE recebe.situacao < 30
AND recebe.dt_vencimento > CURDATE()
GROUP BY recebe.cod_empresa, recebe.dt_vencimento;
