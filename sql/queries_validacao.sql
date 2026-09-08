-- ============================================================
-- Queries de validação — Amazon Athena
-- Banco: state_of_data_db
-- Usadas para validar os 8 insights de negócio documentados em
-- results/README.md, com cross-check contra os notebooks (pandas).
-- ============================================================

-- 1. Adoção de LLM por ano (insight #2)
SELECT * FROM adocao_ia ORDER BY ano_pesquisa;

-- 2. Gênero por nível de senioridade, com percentual (insight #1)
SELECT
    nivel,
    genero,
    n,
    ROUND(100.0 * n / SUM(n) OVER (PARTITION BY nivel), 1) AS percentual
FROM genero_por_nivel
WHERE nivel IS NOT NULL
ORDER BY
    CASE nivel
        WHEN 'Júnior' THEN 1
        WHEN 'Pleno' THEN 2
        WHEN 'Sênior' THEN 3
        WHEN 'Especialista/Staff+' THEN 4
    END,
    genero;

-- 3. Top 5 cargos por mediana salarial (insight #6)
SELECT
    "cargo atual" AS cargo,
    mediana AS salario_mediano,
    n AS respondentes
FROM cargo_salario
WHERE "cargo atual" IS NOT NULL
ORDER BY mediana DESC
LIMIT 5;

-- 4. Evolução salarial por nível e ano (insight #3)
SELECT
    ano_pesquisa,
    nivel,
    mediana AS salario_mediano,
    n AS respondentes
FROM salario_por_nivel
WHERE nivel IN ('Júnior', 'Pleno', 'Sênior')
ORDER BY
    CASE nivel WHEN 'Júnior' THEN 1 WHEN 'Pleno' THEN 2 WHEN 'Sênior' THEN 3 END,
    ano_pesquisa;

-- 5. Salário mediano por nível de ensino (insight #7)
SELECT * FROM ensino_salario ORDER BY mediana DESC;

-- 6. Distribuição de respondentes por região (insight #5)
-- Nota: exclui respondentes que não informaram a região, para refletir
-- o percentual real entre quem respondeu (62,3% Sudeste — ver results/README.md)
SELECT "regiao onde mora" AS regiao, SUM(n) AS total
FROM regiao_por_nivel
WHERE "regiao onde mora" IS NOT NULL
GROUP BY "regiao onde mora"
ORDER BY total DESC;

-- 7. Modelo de trabalho por ano (insight #8)
-- 2023 fica ausente propositalmente: a pergunta não existia naquela edição
-- (ver nota de arquitetura em src/README.md sobre a correção do bug de bypass
-- do Gold Job na camada Raw).
SELECT ano_pesquisa, modelo_trabalho, n
FROM modelo_trabalho
ORDER BY ano_pesquisa, modelo_trabalho;

-- 8. Adoção de tecnologias/linguagens (insight #4)
SELECT * FROM tecnologia ORDER BY pct_adocao DESC;

-- 9. Verificação de schema da tabela modelo_trabalho
-- Usada para confirmar que o Data Catalog reflete a correção arquitetural
-- (coluna deve se chamar "modelo_trabalho", não "modelo_de_trabalho")
DESCRIBE modelo_trabalho;

-- 10. Listagem de tabelas do banco (verificação de integridade do Catalog)
SHOW TABLES IN state_of_data_db;
