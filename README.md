# 🚀 Pipeline ETL Escalável: Processamento de Faturas com PySpark e GCP

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![PySpark](https://img.shields.io/badge/Apache%20Spark-PySpark-orange)
![GCP](https://img.shields.io/badge/GCP-Dataproc%20%7C%20BigQuery%20%7C%20GCS-green)

## 📋 Sobre o Projeto

Este projeto consiste em um pipeline de Engenharia de Dados **ETL (Extract, Transform, Load)** completo, desenvolvido para processar grandes volumes de dados de faturas de varejo (*Retail Invoices*).

O objetivo principal é ingerir dados brutos, aplicar regras de qualidade e negócio, e disponibilizar os dados processados para análise em um Data Warehouse.

Exemplo de Dashboard Simplificado Resultante do Projeto: https://lookerstudio.google.com/s/iHRMmDq2a4I

### Arquitetura da Solução

O fluxo de dados segue a arquitetura moderna de Data Lakehouse na Google Cloud Platform:

1.  **Ingestão (Extract):** Arquivos CSV brutos são carregados no **Google Cloud Storage (GCS)**.
2.  **Processamento (Transform):** Um cluster **Dataproc** executa um job **PySpark** para limpar, tipar e validar os dados.
3.  **Carga (Load):** Os dados tratados são inseridos no **Google BigQuery**, prontos para consumo por ferramentas de BI ou SQL analytics.

```mermaid
graph LR
    A[Arquivo CSV] -->|Upload| B(GCS Bucket - Raw)
    B -->|Leitura| C{Dataproc / PySpark}
    C -->|Limpeza & Regras| C
    C -->|Escrita| D[(BigQuery - Tabela Final)]

