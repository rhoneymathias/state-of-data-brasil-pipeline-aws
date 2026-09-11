# Tech Challenge Fase 3 — State of Data Brasil: Pipeline de Dados na AWS

Pipeline de dados na nuvem, construído sobre AWS Glue (PySpark) e Amazon Athena, que reconcilia três edições da pesquisa **State of Data Brasil** (2023, 2024, 2025-2026) em uma base unificada e gera 8 tabelas analíticas com insights de negócio sobre o mercado brasileiro de dados e IA.

Projeto desenvolvido como Tech Challenge da Fase 3 da pós-graduação em Data Analytics.

## O desafio de dados

As três edições da pesquisa usam convenções de nomenclatura de coluna completamente diferentes entre si, o que impede uma simples concatenação dos arquivos. Foi necessário construir um **crosswalk de reconciliação de schema**: identificar, por meio do texto de cada pergunta, quais colunas de anos diferentes representam a mesma informação.

- **1.190** colunas brutas nas três bases originais
- **215** perguntas identificadas como presentes nas três edições
- **172** perguntas consideradas seguras para merge automático (sem ambiguidade)
- **1** exceção tratada manualmente (`modelo_trabalho`, ausente na edição 2023)

## Arquitetura

Pipeline em arquitetura medalhão (Raw → Bronze → Silver → Gold), com AWS Glue processando os dados em PySpark e o Amazon Athena servindo consultas analíticas SQL sobre o resultado final.

Ver diagrama completo em [`architecture/`](./architecture).

| Componente | Descrição |
|---|---|
| **S3** | `tech-challenge-658801947656` (us-east-1) — armazena as camadas Raw, Bronze, Silver e Gold |
| **Glue Jobs** | `silver-state-of-data` e `gold-state-of-data` (PySpark) |
| **Glue Crawler** | `gold-state-of-data-crawler` — cataloga as tabelas Gold para consulta via Athena |
| **Glue Data Catalog** | `state_of_data_db` — 8 tabelas de negócio |
| **Athena** | consultas SQL de validação e exploração |

## Estrutura do repositório

```
tech-challenge-fase3/
├── architecture/    # Diagrama da arquitetura (draw.io + PNG)
├── data/            # Origem dos dados e links de download (Kaggle)
├── evidence/        # Capturas de tela do pipeline em execução na AWS
├── notebooks/       # EDA e notebook consolidado de scripts/análises
├── results/         # Insights de negócio e gráficos gerados
├── sql/             # Queries Athena usadas na validação e análise
└── src/             # Scripts finais dos Glue Jobs + crosswalk de reconciliação
```

Cada pasta tem seu próprio README com detalhes específicos.

## Principais insights

Resumo dos 8 insights de negócio extraídos do pipeline — detalhamento completo, com metodologia e números validados, em [`results/README.md`](./results/README.md):

1. Queda na participação feminina ao longo das edições e por nível de senioridade
2. Forte crescimento na adoção de LLMs (20,1% → 52,7% entre 2023 e 2025-2026)
3. Mediana salarial do nível Sênior subiu 40% entre 2023 e 2024
4. Python e SQL dominam com mais de 80% de utilização
5. Concentração regional no Sudeste (62,3% dos respondentes que informaram a região)
6. Cargos mais frequentes não são os mais bem remunerados
7. Correlação entre nível de formação acadêmica e mediana salarial
8. Modelo de trabalho: estabilidade entre 2023-2024, seguida de queda do trabalho 100% remoto (45,7% → 39,7%) e alta do presencial (16,3% → 20,8%) entre 2024 e 2025-2026

## Como reproduzir

1. Baixar os três datasets originais (ver links em [`data/README.md`](./data/README.md))
2. Fazer upload dos CSVs brutos para `s3://<seu-bucket>/data-input/raw-{2023,2024,2025-2026}/`
3. Executar o Glue Job `silver-state-of-data` (script em [`src/silver_state_of_data.py`](./src/silver_state_of_data.py))
4. Executar o Glue Job `gold-state-of-data` (script em [`src/gold_state_of_data.py`](./src/gold_state_of_data.py))
5. Executar o Glue Crawler para catalogar as tabelas Gold
6. Consultar via Athena — queries de exemplo em [`sql/`](./sql)

## Stack

Python · Pandas · PySpark · AWS Glue · AWS S3 · Amazon Athena · AWS Glue Data Catalog
