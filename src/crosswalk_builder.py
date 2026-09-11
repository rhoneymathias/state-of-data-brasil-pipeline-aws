"""
crosswalk_builder.py

Reconciliação de schema entre as três edições da pesquisa State of Data Brasil
(2023, 2024, 2025-2026). Extraído do notebook de EDA (célula 4) como script
standalone, para documentar a lógica de forma isolada e reprodutível fora do
ambiente PySpark/Glue.

Esta é a MESMA lógica que foi portada para PySpark em src/silver_state_of_data.py,
adaptada para operar sobre nomes de coluna posicionais (c0, c1, c2...) e gravar
diretamente em S3/Parquet. Este script em pandas serve como referência de
validação e documentação do algoritmo, não é executado como parte do pipeline
AWS em produção.

Entradas esperadas: dataset_2023, dataset_2024, dataset_2025 (pandas DataFrames
carregados a partir dos CSVs brutos do Kaggle — ver data/README.md).
"""

import pandas as pd
import re
import unicodedata


def parse_coluna_2023(c):
    """
    O dataset de 2023 usa nomes de coluna no formato de tupla Python em string,
    ex: "('P1_b ', 'Genero')". Extrai (codigo, texto_da_pergunta).
    """
    m = re.match(r"^\('([^']*)',\s*'(.*)'\)$", c, re.DOTALL)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return c, c


def parse_coluna_outros(c):
    """
    Os datasets de 2024 e 2025-2026 usam o formato "codigo_texto_da_pergunta",
    ex: "1.b_genero". Extrai (codigo, texto_da_pergunta).
    """
    codigo, sep, texto = c.partition("_")
    return (codigo, texto) if sep else (c, c)


def normaliza(texto):
    """
    Normaliza o texto da pergunta para permitir comparação entre anos:
    minúsculas, sem acentos, sem pontuação.
    """
    texto = texto.lower().strip()
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')
    texto = re.sub(r"[^a-z0-9]+", " ", texto).strip()
    return texto


def construir_crosswalk(dataset_2023, dataset_2024, dataset_2025):
    """
    Constrói o crosswalk completo: para cada pergunta normalizada, registra
    em quais anos ela aparece e sob qual nome de coluna original.

    Retorna:
        df_todas_colunas: DataFrame com uma linha por coluna original de cada ano,
            já com (ano, coluna_original, codigo, pergunta_normalizada).
        perguntas_completas: lista de perguntas presentes nos 3 anos.
        perguntas_seguras: subconjunto de perguntas_completas sem ambiguidade
            (aparecem exatamente uma vez por ano) — seguras para merge automático.
    """
    registros = []
    fontes = [
        ("2023", dataset_2023, parse_coluna_2023),
        ("2024", dataset_2024, parse_coluna_outros),
        ("2025-2026", dataset_2025, parse_coluna_outros),
    ]
    for ano, df, parser in fontes:
        for coluna in df.columns:
            codigo, texto = parser(coluna)
            registros.append({
                "ano": ano,
                "coluna_original": coluna,
                "codigo": codigo,
                "pergunta_normalizada": normaliza(texto),
            })

    df_todas_colunas = pd.DataFrame(registros)

    contagem_anos = df_todas_colunas.groupby("pergunta_normalizada")["ano"].nunique()
    perguntas_completas = contagem_anos[contagem_anos == 3].index.tolist()

    contagem_por_ano_pergunta = (
        df_todas_colunas[df_todas_colunas["pergunta_normalizada"].isin(perguntas_completas)]
        .groupby(["pergunta_normalizada", "ano"])
        .size()
        .reset_index(name="ocorrencias")
    )
    perguntas_ambiguas = set(
        contagem_por_ano_pergunta[contagem_por_ano_pergunta["ocorrencias"] > 1]["pergunta_normalizada"]
    )
    perguntas_seguras = [p for p in perguntas_completas if p not in perguntas_ambiguas]

    return df_todas_colunas, perguntas_completas, perguntas_seguras


def identificar_candidatos_a_alias_perdido(df_todas_colunas):
    """
    Auditoria pendente: aponta perguntas presentes em exatamente 2 dos 3 anos.

    Estes são os candidatos a sofrer do MESMO bug que "modelo_trabalho" tinha
    antes da correção -- a pergunta pode existir no terceiro ano, só que sob
    um rótulo de coluna que não normaliza para o mesmo texto (ex: frase
    completa da pergunta em 2023, em vez do código curto de 2024/2025).

    Isso NÃO confirma o bug -- só aponta onde vale a pena olhar manualmente.
    Uma pergunta presente em só 2 anos pode legitimamente não existir no
    terceiro (pergunta nova, ou removida). O único jeito de saber é abrir o
    CSV bruto do ano que falta e conferir à mão -- exatamente o que deveria
    ter sido feito com modelo_trabalho desde o início, em vez de assumir
    ausência a partir de uma falha de comparação de texto.

    Retorna uma lista de dicts, um por pergunta candidata, com os anos onde
    ela foi encontrada, o ano que falta, e as colunas originais correspondentes
    -- para inspeção manual, não para correção automática.
    """
    contagem_anos = df_todas_colunas.groupby("pergunta_normalizada")["ano"].nunique()
    perguntas_parciais = contagem_anos[contagem_anos == 2].index.tolist()

    todos_os_anos = {"2023", "2024", "2025-2026"}
    candidatos = []
    for pergunta in perguntas_parciais:
        linhas = df_todas_colunas[df_todas_colunas["pergunta_normalizada"] == pergunta]
        anos_presentes = set(linhas["ano"])
        ano_faltante = todos_os_anos - anos_presentes
        candidatos.append({
            "pergunta_normalizada": pergunta,
            "anos_presentes": sorted(anos_presentes),
            "ano_faltante": sorted(ano_faltante)[0] if ano_faltante else None,
            "colunas_originais": linhas[["ano", "coluna_original"]].to_dict("records"),
        })

    # ano_faltante == "2023" é a prioridade -- é onde o mix de convenção de
    # rótulo (curto vs. frase completa) já causou um bug confirmado.
    candidatos.sort(key=lambda c: c["ano_faltante"] != "2023")
    return candidatos


if __name__ == "__main__":
    # Exemplo de uso (requer os três CSVs brutos carregados previamente):
    #
    # dataset_2023 = pd.read_csv("data-input/raw-2023/...")
    # dataset_2024 = pd.read_csv("data-input/raw-2024/...")
    # dataset_2025 = pd.read_csv("data-input/raw-2025-2026/...")
    #
    # df_todas_colunas, perguntas_completas, perguntas_seguras = construir_crosswalk(
    #     dataset_2023, dataset_2024, dataset_2025
    # )
    # print(f"Total de colunas processadas: {len(df_todas_colunas)}")
    # print(f"Perguntas presentes nos 3 anos: {len(perguntas_completas)}")
    # print(f"Perguntas seguras (sem ambiguidade): {len(perguntas_seguras)}")
    #
    # candidatos = identificar_candidatos_a_alias_perdido(df_todas_colunas)
    # print(f"\nCandidatos a alias perdido (presentes em só 2 dos 3 anos): {len(candidatos)}")
    # for c in candidatos:
    #     print(f"  falta em {c['ano_faltante']}: {c['pergunta_normalizada']!r}")
    #     print(f"    -> {c['colunas_originais']}")
    pass
