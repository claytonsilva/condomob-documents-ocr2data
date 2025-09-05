from pandas import DataFrame, Series

from processors.docling_analytical import (
    get_current_title,
    identify_row,
)
from utils.constants import ExtracTypeRow

# Teste quando a própria linha é um título


def test_get_current_title_linha_titulo():
    data = {
        "tipoDado": [
            ExtracTypeRow.OTHERS,
            ExtracTypeRow.TITLE,
            ExtracTypeRow.OTHERS,
        ],
        "Data": ["data0", "data1", "data2"],
    }
    table = DataFrame(data)
    row = table.loc[1]  # Linha com título
    result = get_current_title(table, row)
    assert result == "data1"


# Teste quando a linha não é título, mas há um título em uma linha anterior


def test_get_current_title_titulo_anterior():
    data = {
        "tipoDado": [
            ExtracTypeRow.TITLE,
            ExtracTypeRow.OTHERS,
            ExtracTypeRow.OTHERS,
        ],
        "Data": ["first_title", "data1", "data2"],
    }
    table = DataFrame(data)
    # Linha que não é título, mas deve encontrar o título na linha 0
    row = table.loc[2]
    result = get_current_title(table, row)
    assert result == "first_title"


# Teste quando não há nenhum título na faixa analisada


def test_get_current_title_sem_titulo():
    data = {
        "tipoDado": [
            ExtracTypeRow.OTHERS,
            ExtracTypeRow.OTHERS,
            ExtracTypeRow.OTHERS,
        ],
        "Data": ["data0", "data1", "data2"],
    }
    table = DataFrame(data)
    row = table.loc[2]
    result = get_current_title(table, row)
    assert result is None


# Teste com múltiplos títulos para garantir que o título mais próximo seja retornado


def test_get_current_title_multiplos_titulos():
    data = {
        "tipoDado": [
            ExtracTypeRow.TITLE,
            ExtracTypeRow.TITLE,
            ExtracTypeRow.OTHERS,
            ExtracTypeRow.TITLE,
        ],
        "Data": ["first", "second", "data", "third"],
    }
    table = DataFrame(data)
    # Ao passar a linha de índice 2, a iteração ocorre em [2, 1, 0] e encontra o título da linha 1 primeiro.
    row = table.loc[2]
    result = get_current_title(table, row)
    assert result == "second"


# Teste 1.1: Primeira coluna vazia, última coluna vazia e segunda coluna vazia → TRUNCATED_PARTICIPANT


# Teste 1.3: Primeira coluna vazia, mas última coluna NÃO vazia → OTHERS


def test_identify_row_others_primeiro_grupo():
    line = Series(
        {
            "Data": "   ",
            "Descrição": "   ",
            "Participante": "irrelevante",
            "Valor": "Valor não vazio",
        }
    )
    assert identify_row(line) == ExtracTypeRow.OTHERS


# Teste 2.1: Coluna "Data" não vazia, as demais vazias, e valor da "Data" corresponde ao padrão TOTAL → TOTAL


def test_identify_row_total():
    line = Series(
        {
            "Data": "TOTAL: 123.45 Extra",
            "Descrição": "   ",
            "Participante": "   ",
            "Valor": "   ",
        }
    )
    assert identify_row(line) == ExtracTypeRow.TOTAL


# Teste 2.2: Coluna "Data" não vazia, as demais vazias, e valor da "Data" corresponde ao padrão TITLE → TITLE


def test_identify_row_title():
    line = Series(
        {
            "Data": "1.1 - Título Exemplo",
            "Descrição": "   ",
            "Participante": "   ",
            "Valor": "   ",
        }
    )
    assert identify_row(line) == ExtracTypeRow.TITLE


# Teste 2.3: Coluna "Data" não vazia, as demais vazias, mas "Data" não bate com TOTAL nem TITLE → OTHERS


def test_identify_row_others_segundo_grupo():
    line = Series(
        {
            "Data": "Texto aleatório",
            "Descrição": "   ",
            "Participante": "   ",
            "Valor": "   ",
        }
    )
    assert identify_row(line) == ExtracTypeRow.OTHERS


# Teste 3.1: Caso especial no else: "Data" não vazia, mas não preenche a condição do grupo 2, e "Data" é exatamente "Data" → HEADERS


def test_identify_row_headers():
    line = Series(
        {
            "Data": "Data",
            "Descrição": "Conteúdo não vazio",  # quebra a condição de todas vazias
            "Participante": "   ",
            "Valor": "   ",
        }
    )
    assert identify_row(line) == ExtracTypeRow.HEADERS


# Teste 3.2: Caso no else: "Data" não vazia, não se enquadra nos demais padrões e não é "Data" → ROW


def test_identify_row_row():
    line = Series(
        {
            "Data": "05/10/2022",
            "Descrição": "Conteúdo",
            "Participante": "Conteúdo",
            "Valor": "Conteúdo",
        }
    )
    assert identify_row(line) == ExtracTypeRow.ROW
