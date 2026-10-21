# Pipeline de Processamento Distribuído com PySpark no Databricks

Este projeto integra o Módulo 2 da jornada de Engenharia de Dados. O objetivo principal é demonstrar o uso do **Apache Spark (PySpark)** no ambiente **Databricks (Serverless Compute)** para simular o tratamento e classificação de risco de transações financeiras em escala de Big Data.

---
``
## Tecnologias e Ferramentas Utilizadas

- **Linguagem:** Python
- **Engine de Processamento Distribuído:** Apache Spark (`pyspark.sql`)
- **Plataforma Cloud:** Databricks (Serverless Compute)
- **Estruturas de Dados:** PySpark DataFrames & Spark Temporary Views
- **Controle de Versão:** Git & GitHub

---

## Arquitetura e Regras de Negócio Implementadas

1. **Ingestão Distribuída:** Criação de um PySpark DataFrame contendo dados sintéticos de transações bancárias (`PIX`, `TED`, `BOLETO`, `CARTAO`).
2. **Filtragem de Relevância:** Descarte automático de transações abaixo de R$ 1.000,00 (`filter`).
3. **Classificação de Risco (Enriquecimento):** Aplicação de regras condicionais (`when` / `otherwise`) para definir o risco da transação:
   - **ALTO:** Transações acima de R$ 10.000,00.
   - **MEDIO:** Transações entre R$ 1.000,00 e R$ 10.000,00.
4. **Auditoria e Monitoramento:** Criação de carimbo de data/hora (`current_timestamp`) indicando exatamente quando o lote de dados foi processado.
5. **Persistência em Memória (Serverless):** Registro do DataFrame como uma `TempView` (`vw_transacoes_risco`) otimizada para consultas distribuídas no Databricks.

---

## Estrutura do Projeto

```text
processamento-bigdata-pyspark/
├── 01_pyspark_basico.py   # Script principal exportado do Databricks
├── .gitignore            # Regras de exclusao de arquivos temporarios Python
├── LICENSE               # Licenca MIT
└── README.md             # Documentacao do projeto
