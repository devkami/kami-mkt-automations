SELECT MAX(pedido.dt_implant) AS 'ultimo_pedido'
FROM vd_pedido_item AS pedido_item
INNER JOIN vd_pedido AS pedido 
  ON (pedido.cod_pedido = pedido_item.cod_pedido
  AND pedido.nop IN ("6.102","6.404","BLACKFRIDAY","VENDA","VENDA_S_ESTOQUE","WORKSHOP", "BONIFICADO")
  AND pedido.situacao >= 20
  AND pedido.situacao < 200)
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