import sys
import re
import unicodedata
from collections import defaultdict
from functools import reduce

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET = "s3://tech-challenge-658801947656"
RAW = {
    "2023": f"{BUCKET}/data-input/raw-2023/",
    "2024": f"{BUCKET}/data-input/raw-2024/",
    "2025-2026": f"{BUCKET}/data-input/raw-2025-2026/",
}

def parse_coluna_2023(c):
    m = re.match(r"^\('([^']*)',\s*'(.*)'\)$", c, re.DOTALL)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return c, c

def parse_coluna_outros(c):
    codigo, sep, texto = c.partition("_")
    return (codigo, texto) if sep else (c, c)

def normaliza(texto):
    texto = texto.lower().strip()
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')
    texto = re.sub(r"[^a-z0-9]+", " ", texto).strip()
    return texto

opcoes_leitura = {"header": True, "multiLine": True, "escape": '"'}
df_2023_raw = spark.read.options(**opcoes_leitura).csv(RAW["2023"])
df_2024_raw = spark.read.options(**opcoes_leitura).csv(RAW["2024"])
df_2025_raw = spark.read.options(**opcoes_leitura).csv(RAW["2025-2026"])

dfs_raw = {"2023": df_2023_raw, "2024": df_2024_raw, "2025-2026": df_2025_raw}
parsers = {"2023": parse_coluna_2023, "2024": parse_coluna_outros, "2025-2026": parse_coluna_outros}

dfs = {}
colunas_originais_por_ano = {}
resumo_linhas = []
for ano, df_raw in dfs_raw.items():
    originais = df_raw.columns
    colunas_originais_por_ano[ano] = originais
    nomes_seguros = [f"c{i}" for i in range(len(originais))]
    dfs[ano] = df_raw.toDF(*nomes_seguros)

    dfs[ano].write.mode("overwrite").parquet(f"{BUCKET}/bronze/{ano}/")
    resumo_linhas.append(f"bronze/{ano}: {dfs[ano].count()} linhas, {len(dfs[ano].columns)} colunas")

linhas_mapeamento = []
for ano, originais in colunas_originais_por_ano.items():
    for idx, nome_original in enumerate(originais):
        linhas_mapeamento.append((ano, f"c{idx}", nome_original))
df_mapeamento = spark.createDataFrame(linhas_mapeamento, ["ano_pesquisa", "coluna_bronze", "coluna_original"])
df_mapeamento.write.mode("overwrite").parquet(f"{BUCKET}/bronze/_mapeamento_colunas/")
resumo_linhas.append(f"bronze/_mapeamento_colunas: {df_mapeamento.count()} linhas")

por_pergunta = defaultdict(lambda: defaultdict(list))
for ano, originais in colunas_originais_por_ano.items():
    parser = parsers[ano]
    for idx, coluna in enumerate(originais):
        codigo, texto = parser(coluna)
        pergunta = normaliza(texto)
        por_pergunta[pergunta][ano].append(idx)

perguntas_completas = [p for p, anos in por_pergunta.items() if len(anos) == 3]
perguntas_seguras = [
    p for p in perguntas_completas
    if all(len(por_pergunta[p][ano]) == 1 for ano in ["2023", "2024", "2025-2026"])
]

# Excecao manual: "modelo_trabalho" nao entra no crosswalk automatico via
# perguntas_seguras porque o rotulo de 2023 e a frase inteira da pergunta
# ("Atualmente qual a sua forma de trabalho?"), enquanto 2024/2025 usam um
# codigo curto ("modelo_de_trabalho_atual"). Normalizado, o texto de 2023
# nunca bate com o de 2024/2025 -- por isso falha o filtro len(anos) == 3.
#
# CORRECAO: a pergunta EXISTE nos 3 anos (confirmado no CSV bruto de 2023).
# Nao e ausencia de dado, e falha de reconciliacao de schema. Documentado
# aqui como excecao manual e auditavel -- nao alteramos a logica generica
# de normalizacao, que arriscaria fundir outras perguntas por engano.
#
# Os rotulos de categoria (ex: "Modelo 100% remoto") ja sao identicos,
# palavra por palavra, entre os 3 anos -- confirmado via value_counts no
# dado bruto. Nao ha necessidade de harmonizar valores, so de localizar a
# coluna certa em cada ano.
COLUNAS_MODELO_TRABALHO = {
    "2023": "('P2_r ', 'Atualmente qual a sua forma de trabalho?')",
    "2024": "2.r_modelo_de_trabalho_atual",
    "2025-2026": "2.q_modelo_de_trabalho_atual",
}

tabelas = []
for ano in ["2023", "2024", "2025-2026"]:
    df = dfs[ano]

    colunas_select = [df[f"c{por_pergunta[pergunta][ano][0]}"].alias(pergunta) for pergunta in perguntas_seguras]

    if ano in COLUNAS_MODELO_TRABALHO:
        nome_original_modelo = COLUNAS_MODELO_TRABALHO[ano]
        idx_modelo = colunas_originais_por_ano[ano].index(nome_original_modelo)
        colunas_select.append(df[f"c{idx_modelo}"].alias("modelo_trabalho"))
    else:
        colunas_select.append(F.lit(None).cast("string").alias("modelo_trabalho"))

    df_sel = df.select(*colunas_select).withColumn("ano_pesquisa", F.lit(ano))
    tabelas.append(df_sel)

df_silver = reduce(lambda a, b: a.unionByName(b), tabelas)

resumo_linhas.append(f"Perguntas completas (3 anos): {len(perguntas_completas)}")
resumo_linhas.append(f"Perguntas seguras (sem ambiguidade): {len(perguntas_seguras)}")
resumo_linhas.append("Coluna manual adicionada: modelo_trabalho (presente nos 3 anos; 2023 via alias manual -- rotulo de coluna divergente, nao ausencia de dado)")
resumo_linhas.append(f"Silver final: {df_silver.count()} linhas, {len(df_silver.columns)} colunas")

resumo = "\n".join(resumo_linhas)

caminho_resumo = f"{BUCKET}/silver/_resumo_execucao/"
hadoop_conf = sc._jsc.hadoopConfiguration()
jvm_path = sc._jvm.org.apache.hadoop.fs.Path(caminho_resumo)
fs = jvm_path.getFileSystem(hadoop_conf)
if fs.exists(jvm_path):
    fs.delete(jvm_path, True)

sc.parallelize([resumo]).coalesce(1).saveAsTextFile(caminho_resumo)

df_silver.write.mode("overwrite").parquet(f"{BUCKET}/silver/state_of_data_unificado/")

job.commit()