from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, trim
import sys

# ============================================
# CONFIGURAÇÕES
# ============================================
PROJECT_ID = "pyspark-gcp-invoice-project" 
DATASET_ID = "invoice_dataset"
TABLE_NAME = "online_retail_processed"
BUCKET_NAME = "invoice-data-1765885972" 
GCS_INPUT_PATH = f"gs://{BUCKET_NAME}/invoices.csv"

# ============================================
# CRIAR SPARK SESSION
# ============================================
spark = SparkSession.builder \
    .appName("InvoiceProcessing") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .config("spark.dataproc.managed.option.name", "spark-bigquery-dataset-id") \
    .config("spark.dataproc.managed.option.value", DATASET_ID) \
    .getOrCreate()

# ============================================
# 1. LEITURA DOS DADOS
# ============================================
print("📖 Lendo dados do Cloud Storage...")
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(GCS_INPUT_PATH)

df.show(5)
print(f"✅ Total de registros lidos: {df.count()}")

# ============================================
# 2. TRANSFORMAÇÕES
# ============================================
print("🔄 Iniciando transformações...")

# 2.1 Remover espaços em branco
df_clean = df.select(
    col("invoiceno").alias("invoiceno"),
    trim(col("customerid")).alias("customerid"),
    col("invoicedate").alias("invoicedate"),
    col("quantity").cast("double").alias("quantity"),
    upper(trim(col("invoicetype"))).alias("invoicetype")
)

# 2.2 Remover registros com valores NULL em campos críticos
df_clean = df_clean.dropna(subset=["invoiceno", "quantity"])

# 2.3 Adicionar coluna derivada: categoria de valor
df_clean = df_clean.withColumn(
    "quantity_category",
    when(col("quantity") < 1000, "Baixo")
    .when(col("quantity") < 2000, "Médio")
    .otherwise("Alto")
)

# 2.4 Adicionar coluna derivada: is_valid (apenas registros sem 'Return')
df_clean = df_clean.withColumn(
    "is_valid",
    when(col("invoicetype") != "Return", True).otherwise(False)
)

print("✅ Transformações concluídas!")
df_clean.show(5)

# ============================================
# 3. VALIDAÇÕES
# ============================================
print("🔍 Validando dados...")

# Contar registros por invoicetype
invoicetype_count = df_clean.groupBy("invoicetype").count()
print("Registros por invoicetype:")
invoicetype_count.show()

# Validar se existem registros válidos
valid_count = df_clean.filter(col("is_valid") == True).count()
print(f"✅ Total de registros válidos: {valid_count}")

# ============================================
# 4. ESCRITA EM BIGQUERY
# ============================================
print("💾 Escrevendo dados no BigQuery...")

df_clean.write \
    .format("bigquery") \
    .mode("overwrite") \
    .option("table", f"{PROJECT_ID}:{DATASET_ID}.{TABLE_NAME}") \
    .option("temporaryGcsBucket", BUCKET_NAME) \
    .save()

print("✅ Dados salvos com sucesso no BigQuery!")

# ============================================
# 5. LEITURA DE CONFIRMAÇÃO
# ============================================
print("📊 Confirmando escrita...")
df_bigquery = spark.read \
    .format("bigquery") \
    .option("table", f"{PROJECT_ID}:{DATASET_ID}.{TABLE_NAME}") \
    .load()

print(f"✅ Total de registros no BigQuery: {df_bigquery.count()}")
df_bigquery.show(5)

# ============================================
# FIM
# ============================================
spark.stop()
print("✅ Job concluído com sucesso!")
