# CÉLULA 1: Geração de Big Data Sintético com Taxonomia Clínica Realista (Lombossacra, Cervical e Torácica)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

NUM_REGISTROS = 500000

niveis_lesao = [
    "Cervical (C1-C4)", 
    "Cervical (C5-C8)", 
    "Toracica (T1-T6)", 
    "Toracica (T7-T12)", 
    "Lombar/Lombossacra (L5-S1)"
]

causas = [
    "Hernia Discual Aguda / Cauda Equina", 
    "Acidente Automobilistico", 
    "Queda de Altura", 
    "Agressao/Trauma Directo", 
    "Causa Nao-Traumatica"
]

sintomas_iniciais = [
    "Anestesia em Sela (Perianal/Perineo)", 
    "Retencao Urinaria e Intestinal", 
    "Perda de Forca / Atrofia Panturrilha (S1)",
    "Dor Ciatica Grave / Parestesia Calcanhar", 
    "Perda Motora Aguda Membros Inferiores", 
    "Parestesia Gradual"
]

sequelas = [
    "Dedos em Garra / Alteracao de Marcha (Claudicacao)",
    "Atrofia da Panturrilha / Perda Flexao Plantar",
    "Anestesia em Sela Residual", 
    "Bexiga Neurogenica / Sondagem", 
    "Paraplegia Completa"
]

classificacao_asia = ["ASIA A (Completa)", "ASIA B (Incompleta)", "ASIA C (Incompleta)", "ASIA D (Incompleta)"]

df_base = spark.range(0, NUM_REGISTROS).withColumnRenamed("id", "id_atendimento")

df_saude_raw = df_base \
    .withColumn("id_paciente", F.concat(F.lit("PAC-"), F.expr("id_atendimento % 100000"))) \
    .withColumn("idade", (F.rand() * 65 + 15).cast("int")) \
    .withColumn("nivel_lesao", F.element_at(F.array([F.lit(x) for x in niveis_lesao]), (F.rand() * len(niveis_lesao) + 1).cast("int"))) \
    .withColumn("causa_lesao", F.element_at(F.array([F.lit(x) for x in causas]), (F.rand() * len(causas) + 1).cast("int"))) \
    .withColumn("sintoma_inicial", F.element_at(F.array([F.lit(x) for x in sintomas_iniciais]), (F.rand() * len(sintomas_iniciais) + 1).cast("int"))) \
    .withColumn("sequela_principal", F.element_at(F.array([F.lit(x) for x in sequelas]), (F.rand() * len(sequelas) + 1).cast("int"))) \
    .withColumn("classificacao_asia", F.element_at(F.array([F.lit(x) for x in classificacao_asia]), (F.rand() * len(classificacao_asia) + 1).cast("int"))) \
    .withColumn("score_mif", (F.rand() * 108 + 18).cast("int")) \
    .withColumn("dias_internacao", (F.rand() * 120 + 5).cast("int"))

df_saude_raw.display()


# CÉLULA 2: Regras de Negócio, Janela Cirúrgica Urgente (24h-48h) e Custos Hospitalares

df_saude_processado = df_saude_raw \
    .withColumn(
        "protocolo_atendimento",
        F.when(
            (F.col("nivel_lesao").contains("L5-S1")) & 
            ((F.col("sintoma_inicial").contains("Anestesia em Sela")) | (F.col("sintoma_inicial").contains("Retencao Urinaria"))),
            "RED_FLAG_CAUDA_EQUINA_CIRURGIA_URGENTE"
        )
        .when(
            (F.col("nivel_lesao").contains("Cervical")) & (F.col("classificacao_asia") == "ASIA A (Completa)"), 
            "EMERGENCIA_UTI_DESCOMPRESSAO"
        )
        .when(F.col("classificacao_asia").startswith("ASIA A"), "ALTA_COMPLEXIDADE_CIRURGICA")
        .otherwise("TRATAMENTO_CONSERVADOR_REHAB")
    ) \
    .withColumn(
        # Define a janela cirúrgica recomendada em horas conforme a celeridade médica
        "janela_cirurgica_alvo",
        F.when(F.col("protocolo_atendimento") == "RED_FLAG_CAUDA_EQUINA_CIRURGIA_URGENTE", "24h - 48h (Critico Cauda Equina)")
         .when(F.col("protocolo_atendimento") == "EMERGENCIA_UTI_DESCOMPRESSAO", "24h (Urgencia Cervical)")
         .when(F.col("protocolo_atendimento") == "ALTA_COMPLEXIDADE_CIRURGICA", "72h (Janela Eletiva Urgente)")
         .otherwise("N/A - Tratamento Conservador")
    ) \
    .withColumn(
        # Simula o tempo real decorrido em horas ate o paciente entrar no centro cirurgico (de 6h a 72h)
        "horas_ate_cirurgia",
        F.when(F.col("protocolo_atendimento").contains("REHAB"), None)
         .otherwise((F.rand() * 66 + 6).cast("int"))
    ) \
    .withColumn(
        # Avalia se a cirurgia ocorreu dentro da janela critica para diminuir sequelas
        "status_janela_critica",
        F.when(F.col("horas_ate_cirurgia").isNull(), "NÃO_CIRÚRGICO")
         .when(F.col("horas_ate_cirurgia") <= 48, "DENTRO_DA_JANELA_IDEAL_48H")
         .otherwise("ALERTA_ESTOUROU_JANELA_48H")
    ) \
    .withColumn(
        "diaria_estimada",
        F.when(F.col("protocolo_atendimento").startswith("RED_FLAG"), 4500.00)
         .when(F.col("protocolo_atendimento").contains("EMERGENCIA"), 3800.00)
         .otherwise(1200.00)
    ) \
    .withColumn(
        "custo_total_internacao",
        F.round(F.col("dias_internacao") * F.col("diaria_estimada"), 2)
    ) \
    .withColumn(
        "data_processamento",
        F.current_timestamp()
    )

# Exibe as colunas principais com foco na celeridade da cirurgia
df_saude_processado.select(
    "id_paciente",
    "nivel_lesao",
    "sintoma_inicial",
    "protocolo_atendimento",
    "janela_cirurgica_alvo",
    "horas_ate_cirurgia",
    "status_janela_critica",
    "dias_internacao"
).display()

# CÉLULA 3: Análises Avançadas de Big Data com Window Functions

from pyspark.sql.window import Window
from pyspark.sql import functions as F

# 1. Definição da Janela Particionada por Nível de Lesão e Ordenada por Dias de Internação (Decrescente)
janela_lesao_dias = Window.partitionBy("nivel_lesao").orderBy(F.col("dias_internacao").desc())

# 2. Definição da Janela Apenas Particionada (Sem ordenação, para calcular médias globais do grupo)
janela_lesao_grupo = Window.partitionBy("nivel_lesao")

# 3. Aplicação das Window Functions no DataFrame Enriquecido
df_saude_window = df_saude_processado \
    .withColumn(
        "rank_internacao_no_nivel",
        F.row_number().over(janela_lesao_dias)
    ) \
    .withColumn(
        "media_dias_nivel_lesao",
        F.round(F.avg("dias_internacao").over(janela_lesao_grupo), 1)
    ) \
    .withColumn(
        "diferenca_vs_media_nivel",
        F.round(F.col("dias_internacao") - F.col("media_dias_nivel_lesao"), 1)
    )

# 4. Filtrar para visualizar o TOP 3 casos com maior tempo de internação por nível de lesão
df_top_casos = df_saude_window.filter(F.col("rank_internacao_no_nivel") <= 3)

# Exibe os casos mais graves ranqueados
df_top_casos.select(
    "nivel_lesao",
    "rank_internacao_no_nivel",
    "id_paciente",
    "sintoma_inicial",
    "protocolo_atendimento",
    "dias_internacao",
    "media_dias_nivel_lesao",
    "diferenca_vs_media_nivel",
    "custo_total_internacao"
).display()


# CÉLULA 4: Escrita Otimizada em Camada Silver/Gold (Formato Delta/Parquet Particionado)

# 1. Salvar a tabela em formato Delta particionada por 'protocolo_atendimento'
(
    df_saude_window
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("protocolo_atendimento")
    .saveAsTable("db_saude_lesao_medular_gold")
)

print("Pipeline executado com sucesso! Tabela Delta 'db_saude_lesao_medular_gold' salva com 500.000 registros.")
