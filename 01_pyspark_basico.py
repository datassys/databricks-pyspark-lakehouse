# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp

# 1. Dados simulados
dados_transacoes = [
    (101, "PIX", 1500.00, "RJ", "2024-03-01"),
    (102, "CARTAO", 85.50, "SP", "2024-03-01"),
    (103, "TED", 12000.00, "RJ", "2024-03-02"),
    (104, "PIX", 450.00, "MG", "2024-03-02"),
    (105, "BOLETO", 3200.00, "SP", "2024-03-03"),
    (106, "PIX", 25000.00, "RJ", "2024-03-03")
]

colunas = ["id_transacao", "tipo_pagamento", "valor", "estado", "data_transacao"]

# 2. Criando o DataFrame distribuído no PySpark
df_transacoes = spark.createDataFrame(dados_transacoes, schema=colunas)

# 3. Aplicando as transformações de negócio
df_processado = df_transacoes \
    .filter(col("valor") > 1000) \
    .withColumn("categoria_risco", when(col("valor") > 10000, "ALTO").otherwise("MEDIO")) \
    .withColumn("dt_processamento", current_timestamp())

# 4. Registrando o DataFrame como uma Tabela Temporária no Spark (Padrão Serverless)
df_processado.createOrReplaceTempView("vw_transacoes_risco")

# 5. Lendo da Tabela Temporária
df_final = spark.table("vw_transacoes_risco")

# Exibe o resultado final na tela
display(df_final)