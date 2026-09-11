import sys
import re
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

silver = spark.read.parquet(f"{BUCKET}/silver/state_of_data_unificado/")

ponto_medio = {
    "Menos de R$ 1.000/mês": 500,
    "de R$ 1.001/mês a R$ 2.000/mês": 1500,
    "de R$ 2.001/mês a R$ 3.000/mês": 2500,
    "de R$ 3.001/mês a R$ 4.000/mês": 3500,
    "de R$ 4.001/mês a R$ 6.000/mês": 5000,
    "de R$ 6.001/mês a R$ 8.000/mês": 7000,
    "de R$ 8.001/mês a R$ 12.000/mês": 10000,
    "de R$ 12.001/mês a R$ 16.000/mês": 14000,
    "de R$ 16.001/mês a R$ 20.000/mês": 18000,
    "de R$ 20.001/mês a R$ 25.000/mês": 22500,
    "de R$ 25.001/mês a R$ 30.000/mês": 27500,
    "de R$ 30.001/mês a R$ 40.000/mês": 35000,
    "Acima de R$ 40.001/mês": 45000,
}
mapa_expr = F.create_map([F.lit(x) for pair in ponto_medio.items() for x in pair])
silver = silver.withColumn("salario_estimado", mapa_expr[F.col("faixa salarial")])

resumo_linhas = [f"Silver lida: {silver.count()} linhas, {len(silver.columns)} colunas"]

# 1. GENERO POR NIVEL
gold_genero_nivel = silver.groupBy("nivel", "genero").agg(F.count("*").alias("n"))
gold_genero_nivel.write.mode("overwrite").parquet(f"{BUCKET}/gold/genero_por_nivel/")
resumo_linhas.append(f"gold/genero_por_nivel: {gold_genero_nivel.count()} linhas")

# 2. SALARIO POR NIVEL, POR ANO
gold_salario_nivel = (
    silver.groupBy("ano_pesquisa", "nivel")
    .agg(
        F.expr("percentile_approx(salario_estimado, 0.5)").alias("mediana"),
        F.avg("salario_estimado").alias("media"),
        F.count("salario_estimado").alias("n"),
    )
)
gold_salario_nivel.write.mode("overwrite").parquet(f"{BUCKET}/gold/salario_por_nivel/")
resumo_linhas.append(f"gold/salario_por_nivel: {gold_salario_nivel.count()} linhas")

# 3. ADOCAO DE IA (LLM), POR ANO
col_ia = "utilizo llm s para solucionar problemas de negocio"
if col_ia in silver.columns:
    gold_ia = (
        silver.groupBy("ano_pesquisa")
        .agg(
            F.sum(F.col(col_ia).cast("double")).alias("usa_llm"),
            F.count(col_ia).alias("respondentes_bloco"),
        )
        .withColumn("pct_adocao", F.round(F.col("usa_llm") / F.col("respondentes_bloco") * 100, 1))
    )
    gold_ia.write.mode("overwrite").parquet(f"{BUCKET}/gold/adocao_ia/")
    resumo_linhas.append(f"gold/adocao_ia: {gold_ia.count()} linhas")
else:
    resumo_linhas.append("gold/adocao_ia: coluna nao encontrada na Silver - PULADO")

# 4. TECNOLOGIA (linguagens)
colunas_tech = [c for c in ["python", "r", "sql", "scala", "julia", "rust", "c c c", "sql server"] if c in silver.columns]
linhas_tech = []
for c in colunas_tech:
    agg = silver.agg(
        F.sum(F.col(c).cast("double")).alias("usa"),
        F.count(c).alias("respondentes"),
    ).collect()[0]
    linhas_tech.append((c, agg["usa"], agg["respondentes"]))
gold_tech = spark.createDataFrame(linhas_tech, ["tecnologia", "usa", "respondentes_bloco"]) \
    .withColumn("pct_adocao", F.round(F.col("usa") / F.col("respondentes_bloco") * 100, 1))
gold_tech.write.mode("overwrite").parquet(f"{BUCKET}/gold/tecnologia/")
resumo_linhas.append(f"gold/tecnologia: {gold_tech.count()} linhas")

# 5. REGIAO x SENIORIDADE
if "regiao onde mora" in silver.columns:
    gold_regiao_nivel = silver.groupBy("regiao onde mora", "nivel").agg(F.count("*").alias("n"))
    gold_regiao_nivel.write.mode("overwrite").parquet(f"{BUCKET}/gold/regiao_por_nivel/")
    resumo_linhas.append(f"gold/regiao_por_nivel: {gold_regiao_nivel.count()} linhas")

# 6. CARGO x SALARIO
if "cargo atual" in silver.columns:
    gold_cargo_salario = (
        silver.groupBy("cargo atual")
        .agg(
            F.expr("percentile_approx(salario_estimado, 0.5)").alias("mediana"),
            F.count("*").alias("n"),
        )
        .filter(F.col("n") >= 30)
        .orderBy(F.col("mediana").desc())
    )
    gold_cargo_salario.write.mode("overwrite").parquet(f"{BUCKET}/gold/cargo_salario/")
    resumo_linhas.append(f"gold/cargo_salario: {gold_cargo_salario.count()} linhas")

# 7. NIVEL DE ENSINO x SALARIO
if "nivel de ensino" in silver.columns:
    gold_ensino_salario = (
        silver.groupBy("nivel de ensino")
        .agg(
            F.expr("percentile_approx(salario_estimado, 0.5)").alias("mediana"),
            F.count("*").alias("n"),
        )
        .orderBy(F.col("mediana").desc())
    )
    gold_ensino_salario.write.mode("overwrite").parquet(f"{BUCKET}/gold/ensino_salario/")
    resumo_linhas.append(f"gold/ensino_salario: {gold_ensino_salario.count()} linhas")

# 8. MODELO DE TRABALHO
if "modelo_trabalho" in silver.columns:
    gold_modelo = (
        silver.filter(F.col("modelo_trabalho").isNotNull())
        .groupBy("ano_pesquisa", "modelo_trabalho")
        .agg(F.count("*").alias("n"))
    )
    gold_modelo.write.mode("overwrite").parquet(f"{BUCKET}/gold/modelo_trabalho/")
    anos_presentes = [r["ano_pesquisa"] for r in gold_modelo.select("ano_pesquisa").distinct().collect()]
    resumo_linhas.append(
        f"gold/modelo_trabalho: {gold_modelo.count()} linhas "
        f"(anos presentes: {sorted(anos_presentes)} -- 2023 incluido desde a correcao "
        f"do alias na Silver; antes era excluido por engano)"
    )
else:
    resumo_linhas.append("gold/modelo_trabalho: coluna nao encontrada na Silver - PULADO")

resumo = "\n".join(resumo_linhas)

caminho_resumo = f"{BUCKET}/gold/_resumo_execucao/"
hadoop_conf = sc._jsc.hadoopConfiguration()
jvm_path = sc._jvm.org.apache.hadoop.fs.Path(caminho_resumo)
fs = jvm_path.getFileSystem(hadoop_conf)
if fs.exists(jvm_path):
    fs.delete(jvm_path, True)

sc.parallelize([resumo]).coalesce(1).saveAsTextFile(caminho_resumo)

job.commit()