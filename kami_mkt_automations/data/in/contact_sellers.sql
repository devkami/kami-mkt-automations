SELECT DISTINCTROW 
IFNULL(CONVERT(employee.cod_colaborador, UNSIGNED), 0) AS 'id',
IFNULL(CONVERT(employee.nome_colaborador, CHAR), 'NULO') AS 'name',
IFNULL(CONVERT(employee.email, CHAR), 'NULO') AS 'email',
IFNULL(CONVERT(CONCAT('+55', employee.ddd_celular, employee.celular) , CHAR), 'NULO') AS 'phone'
FROM sg_colaborador AS employee
INNER JOIN sg_grupo_colaborador AS employee_group ON employee_group.cod_colaborador = employee.cod_colaborador
WHERE employee_group.cod_grupo  IN (3, 4, 15, 17, 19, 23, 31, 37)
AND employee.email IS NOT NULL
AND employee.ddd_celular IS NOT NULL
AND employee.celular IS NOT NULL;