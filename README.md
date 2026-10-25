# Databricks & PySpark Lakehouse Platform

---

## Visao Geral

Este repositorio centraliza projetos e pipelines de Engenharia de Dados desenvolvidos no ecossistema Databricks, PySpark e Delta Lake. O objetivo e demonstrar arquiteturas escalaveis, processamento distribuido e aplicacao de regras de negocio complexas em cenarios reais de mercado.

---

## Estrutura do Repositorio

* 01_fundamentos_pyspark/
  Pipeline inicial para fundamentacao de transformacoes, selecao de colunas e operacoes basicas de DataFrame com PySpark no Databricks.

* 02_lakehouse_saude_lesao_medular_delta/
  Pipeline de Big Data para 500.000 atendimentos de Saude (Lesao Medular / Cauda Equina), integrando regras de urgencia cirurgica, Window Functions e persistencia otimizada em Delta Lake.

---

## Tecnologias e Arquitetura

* Processamento Distribuido: Apache Spark / PySpark
* Plataforma: Databricks Serverless Compute
* Armazenamento Transactional: Delta Lake (ACID, Parquet, Partitioning)
* Versionamento e Padronizacao: Git com Conventional Commits


