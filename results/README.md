# Results

Resultados visuais e principais insights obtidos a partir da análise das três edições do **State of Data Brasil (2023, 2024 e 2025–2026)**.

Os resultados apresentados nesta pasta foram gerados a partir do pipeline de dados desenvolvido no projeto, envolvendo ingestão, tratamento, padronização, transformação e análise dos dados.

## Principais resultados

### 1. Diversidade e senioridade

A participação feminina apresentou redução ao longo das três edições analisadas, passando de 24,4% em 2023 para 22,0% em 2025–2026.

A análise por nível de carreira também indica menor participação feminina conforme aumenta a senioridade: 28,2% entre profissionais Junior, 24,7% entre Pleno, 21,3% entre Senior e 20,1% entre Specialist/Staff+.

O resultado aponta para uma diferença de representatividade que merece investigação adicional, especialmente em relação à progressão e retenção de profissionais ao longo da carreira.

### 2. Adoção de LLMs

A utilização de Large Language Models (LLMs) apresentou forte crescimento entre os respondentes desse bloco da pesquisa:

* 2023: 20,1%
* 2024: 39,1%
* 2025–2026: 52,7%

O resultado indica uma rápida expansão da adoção de ferramentas baseadas em IA generativa entre os profissionais representados nesse recorte.

> Observação: esse indicador considera apenas os respondentes que participaram desse bloco específico da pesquisa, e não todo o conjunto de profissionais.

### 3. Salários por nível de carreira

Os dados indicam estabilidade nominal nos níveis Junior e Pleno no período analisado, enquanto a mediana salarial do nível Senior apresentou aumento de 40% entre 2023 e 2024, passando de R$ 10 mil para R$ 14 mil e permanecendo nesse patamar na edição seguinte.

Esse comportamento evidencia diferenças relevantes na evolução salarial entre os níveis de carreira.

### 4. Tecnologias mais utilizadas

Python e SQL aparecem como as tecnologias de maior presença entre os respondentes, ambas superando 80% de utilização no recorte analisado.

Após essas duas tecnologias, observa-se uma queda significativa na participação das demais ferramentas e linguagens.

O resultado reforça a relevância de fundamentos de programação e consulta a dados para profissionais da área.

### 5. Região e senioridade

A região Sudeste concentra 62,3% dos respondentes e apresenta também a maior proporção de profissionais em níveis mais elevados de senioridade.

Esse resultado demonstra uma concentração regional relevante da força de trabalho representada pela pesquisa.

### 6. Cargo e remuneração

Data Analyst aparece como o cargo com maior frequência entre os respondentes, mas não apresenta a maior mediana salarial.

Cargos mais especializados, como Data Architect e Machine Learning Engineer, apresentam medianas salariais superiores, embora com volumes menores de respondentes.

O resultado evidencia diferenças de remuneração entre funções e níveis de especialização.

### 7. Educação e remuneração

A análise demonstra associação entre maior nível de formação acadêmica e maiores medianas salariais no conjunto analisado:

* Estudante: R$ 3,5 mil
* Graduação: R$ 7 mil
* Pós-graduação: R$ 10 mil
* Mestrado/Doutorado: R$ 14 mil

Os dados indicam um diferencial salarial associado ao nível de formação, embora a análise não permita afirmar uma relação causal.

### 8. Modelo de trabalho

A distribuição do modelo de trabalho nas três edições:

* 100% Remoto: 46,3% (2023) → 45,7% (2024) → 39,7% (2025–2026)
* 100% Presencial: 16,6% (2023) → 16,3% (2024) → 20,8% (2025–2026)
* Híbrido (dias fixos): 16,6% (2023) → 17,5% (2024) → 20,0% (2025–2026)
* Híbrido (flexível): 20,5% (2023) → 20,5% (2024) → 19,5% (2025–2026)

O trabalho 100% remoto se manteve estável entre 2023 e 2024 e caiu de forma relevante apenas entre 2024 e 2025–2026. O presencial segue o padrão inverso: estável em 2023–2024, com alta concentrada em 2024–2025/26. O híbrido com dias fixos cresce de forma consistente nos três anos, ao contrário do híbrido flexível, que se mantém estável.

O comportamento é compatível com um movimento de retorno ao escritório concentrado em um único intervalo (2024–2025/26), e não com uma tendência linear ao longo de três anos — distinção relevante para quem for projetar o próximo ciclo. Não é possível atribuir causalidade apenas aos dados da pesquisa.pesquisa.

## Metodologia

Os resultados foram produzidos a partir de três edições do State of Data Brasil.

Devido às diferenças de estrutura e nomenclatura entre as edições, foi realizada uma etapa de **reconciliação de schema**, utilizando o texto das perguntas como referência para identificar questões equivalentes entre os anos.

Após a padronização, foram identificadas:

* **1.190 colunas** nas bases originais;
* **215 questões** comuns às três edições;
* **172 questões** consideradas seguras para análise comparativa.

Os dados foram processados utilizando **Python/Pandas, PySpark, AWS Glue e Amazon Athena**, seguindo a arquitetura de dados apresentada no projeto.

## Arquivos

Os arquivos desta pasta correspondem aos gráficos e materiais visuais utilizados para apresentar os principais resultados da análise.

Para consultar o processo completo de análise, acesse os notebooks disponíveis em [`../notebooks/`](../notebooks/).

Para consultar a arquitetura do pipeline, acesse [`../architecture/`](../architecture/).

