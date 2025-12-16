-- Variáveis do Ambiente (Ajuste conforme o seu setup)
-- Substitua os valores abaixo pelo seu ID de Projeto e nomes de Dataset/Tabela.

DECLARE PROJECT_ID STRING DEFAULT 'pyspark-gcp-invoice-project';
DECLARE DATASET_ID STRING DEFAULT 'invoice_dataset';
DECLARE TABLE_NAME STRING DEFAULT 'online_retail_processed';

-- Nome completo da tabela no BigQuery
DECLARE FULL_TABLE_NAME STRING DEFAULT CONCAT(PROJECT_ID, '.', DATASET_ID, '.', TABLE_NAME);


-- =======================================================
-- 1. VERIFICAÇÃO DE DADOS E METADADOS
-- =======================================================
-- Objetivo: Confirmar se a tabela foi carregada corretamente e verificar as primeiras linhas.

SELECT 
    *
FROM 
    `pyspark-gcp-invoice-project.invoice_dataset.online_retail_processed`
LIMIT 100;

-- =======================================================
-- 2. VALIDAÇÃO DE QUALIDADE (DATA QUALITY)
-- =======================================================
-- Objetivo: Checar o resultado da transformação 'is_valid' feita no PySpark.
-- Espera-se que a maioria seja 'true' (Purchases) e uma porção menor 'false' (Returns/etc.).

SELECT
    is_valid,
    COUNT(*) AS total_registros,
    ROUND(COUNT(*) * 100 / (SELECT COUNT(*) FROM `pyspark-gcp-invoice-project.invoice_dataset.online_retail_processed`), 2) AS percentual
FROM
    `pyspark-gcp-invoice-project.invoice_dataset.online_retail_processed`
GROUP BY 1
ORDER BY 2 DESC;

-- =======================================================
-- 3. MÉTRICA DE NEGÓCIO: TOP 5 PAÍSES POR RECEITA
-- =======================================================
-- Objetivo: Demonstrar a capacidade de gerar métricas de negócio.
-- A receita é calculada apenas para transações válidas (is_valid = true).

SELECT
    country,
    -- Cálcula a receita total (Quantity * UnitPrice)
    SUM(quantity * unitprice) AS receita_total
FROM
    `pyspark-gcp-invoice-project.invoice_dataset.online_retail_processed`
WHERE
    is_valid = TRUE
GROUP BY 1
ORDER BY receita_total DESC
LIMIT 5;

-- =======================================================
-- 4. MÉTRICA DE NEGÓCIO: TOP 5 PRODUTOS MAIS VENDIDOS
-- =======================================================
-- Objetivo: Identificar os produtos de maior volume de vendas.

SELECT
    stockcode,
    description,
    SUM(quantity) AS total_unidades_vendidas
FROM
    `pyspark-gcp-invoice-project.invoice_dataset.online_retail_processed`
WHERE
    is_valid = TRUE
GROUP BY 1, 2
ORDER BY total_unidades_vendidas DESC
LIMIT 5;

-- =======================================================
-- 5. VALIDAÇÃO DE INTEGRIDADE: CLIENTES SEM ID
-- =======================================================
-- Objetivo: Checar se existem registros válidos (purchases) sem um CustomerID associado.
-- Isso pode indicar um problema de dados a ser resolvido.

SELECT
    COUNT(*) AS total_transacoes_sem_cliente
FROM
    `pyspark-gcp-invoice-project.invoice_dataset.online_retail_processed`
WHERE
    is_valid = TRUE
    AND customerid IS NULL;


-- =======================================================
-- 6. TENDÊNCIA TEMPORAL: RECEITA POR MÊS (Último ano disponível)
-- =======================================================
-- Objetivo: Mostrar a capacidade de agregar e extrair tendência temporal.

SELECT
    EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', invoicedate)) AS ano,
    EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', invoicedate)) AS mes,
    SUM(quantity * unitprice) AS receita_mensal
FROM
    `pyspark-gcp-invoice-project.invoice_dataset.online_retail_processed`
WHERE
    is_valid = TRUE
GROUP BY 1, 2
ORDER BY 1, 2;