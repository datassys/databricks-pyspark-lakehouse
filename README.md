# Databricks & PySpark Lakehouse Platform

---

## Visão Geral

Este repositório centraliza projetos e pipelines de Engenharia de Dados desenvolvidos no ecossistema Databricks, PySpark e Delta Lake. O objetivo é demonstrar arquiteturas escaláveis, processamento distribuído e aplicação de regras de negócio complexas em cenários reais de mercado.

---

## Estrutura do Repositório

- **`01_fundamentos_pyspark/`**  
  Pipeline inicial para fundamentação de transformações, seleção de colunas e operações básicas de DataFrame com PySpark no Databricks.

- **`02_lakehouse_saude_lesao_medular_delta/`**  
  Pipeline de Big Data para 500.000 atendimentos de Saúde (Lesão Medular / Cauda Equina), integrando regras de urgência cirúrgica, Window Functions e persistência otimizada em Delta Lake.

---

## Tecnologias e Arquitetura

- **Processamento Distribuído:** Apache Spark / PySpark
- **Plataforma:** Databricks Serverless Compute
- **Armazenamento Transacional:** Delta Lake (ACID, Parquet, Partitioning)
- **Versionamento e Padronização:** Git com Conventional Commits
