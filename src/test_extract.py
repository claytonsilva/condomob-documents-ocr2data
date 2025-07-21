from pandas import DataFrame, Series

from constants import ExtracTypeRow
from extract import (
    append_data,
    get_current_title,
    get_next_row_aftertruncated,
    has_truncated_rows,
    identify_row,
    merge_truncated,
)

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


# Testa quando não há nenhuma linha truncada na faixa analisada


def test_has_truncated_rows_sem_truncated():
    data = {
        "tipoDado": [
            ExtracTypeRow.OTHERS,
            ExtracTypeRow.OTHERS,
            ExtracTypeRow.OTHERS,
        ],
    }
    table = DataFrame(data)
    resultado = has_truncated_rows(table, init_index=0)
    assert resultado == -1


# Testa quando a linha truncada é do tipo TRUNCATED_DESCRIPTION


def test_has_truncated_rows_truncated_description():
    data = {
        "tipoDado": [
            ExtracTypeRow.OTHERS,
            ExtracTypeRow.TRUNCATED_DESCRIPTION,
            ExtracTypeRow.OTHERS,
        ],
    }
    table = DataFrame(data)
    resultado = has_truncated_rows(table, init_index=0)
    # A função deve retornar o índice 1, onde ocorre o TRUNCATED_DESCRIPTION
    assert resultado == 1


# Testa quando a linha truncada é do tipo TRUNCATED_PARTICIPANT


def test_has_truncated_rows_truncated_participant():
    data = {
        "tipoDado": [
            ExtracTypeRow.OTHERS,
            ExtracTypeRow.TRUNCATED_PARTICIPANT,
            ExtracTypeRow.OTHERS,
        ],
    }
    table = DataFrame(data)
    resultado = has_truncated_rows(table, init_index=0)
    # Deve retornar o índice 1, onde ocorre o TRUNCATED_PARTICIPANT
    assert resultado == 1


# Testa o uso do parâmetro init_index para ignorar linhas anteriores


def test_has_truncated_rows_com_init_index():
    data = {
        "tipoDado": [
            ExtracTypeRow.TRUNCATED_DESCRIPTION,
            ExtracTypeRow.TRUNCATED_PARTICIPANT,
            ExtracTypeRow.TRUNCATED_DESCRIPTION,
        ],
    }
    table = DataFrame(data)

    # Ao iniciar a partir do índice 1, a função ignora o valor do índice 0
    resultado = has_truncated_rows(table, init_index=1)
    assert resultado == 1

    # Ao iniciar a partir do índice 2, apenas a linha de índice 2 é considerada
    resultado = has_truncated_rows(table, init_index=2)
    assert resultado == 2


# Testa cenário com múltiplas linhas truncadas para garantir que a primeira encontrada seja retornada


def test_has_truncated_rows_multiplos_truncated():
    data = {
        "tipoDado": [
            ExtracTypeRow.OTHERS,
            ExtracTypeRow.TRUNCATED_PARTICIPANT,
            ExtracTypeRow.TRUNCATED_DESCRIPTION,
            ExtracTypeRow.OTHERS,
        ],
    }
    table = DataFrame(data)
    # Iniciando do índice 0, a função deve retornar o índice 1 (primeiro truncado encontrado)
    resultado = has_truncated_rows(table, init_index=0)
    assert resultado == 1


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


# Teste 1: Verifica a concatenação simples de duas strings não vazias.
def test_append_data_basic():
    result = append_data("Hello", "World")
    assert result == "Hello World"


# Teste 2: Verifica quando a string atual está vazia.


def test_append_data_current_empty():
    result = append_data("", "World")
    assert result == "World"


# Teste 3: Verifica quando a nova string está vazia.


def test_append_data_new_empty():
    result = append_data("Hello", "")
    # A função gera "Hello " e o strip() remove o espaço final.
    assert result == "Hello"


# Teste 4: Verifica quando ambas as strings estão vazias.


def test_append_data_both_empty():
    result = append_data("", "")
    assert result == ""


# Teste 5: Verifica o comportamento quando as entradas possuem espaços extras.


def test_append_data_with_whitespace():
    result = append_data("   Hello", "World   ")
    # O resultado esperado é que os espaços adicionais sejam removidos nas extremidades.
    assert result == "Hello World"


# Teste: Verifica se a função retorna o índice da primeira linha com tipo ROW após o init_index.


def test_get_next_row_aftertruncated_found():
    data = {
        "tipoDado": [
            ExtracTypeRow.TRUNCATED_DESCRIPTION,
            ExtracTypeRow.OTHERS,
            ExtracTypeRow.ROW,
            ExtracTypeRow.ROW,
        ]
    }
    table = DataFrame(data)
    # Considerando init_index=1, as linhas com índice > 1 são: 2 e 3.
    # O primeiro com tipo ROW é a linha de índice 2.
    result = get_next_row_aftertruncated(table, init_index=1)
    assert result == 2


# Teste: Verifica se a função retorna -1 quando não há linha do tipo ROW após o init_index.


def test_get_next_row_aftertruncated_not_found():
    data = {
        "tipoDado": [
            ExtracTypeRow.ROW,
            ExtracTypeRow.TRUNCATED_DESCRIPTION,
            ExtracTypeRow.TRUNCATED_PARTICIPANT,
        ]
    }
    table = DataFrame(data)
    # Apesar de a linha de índice 0 ser do tipo ROW, ela não é considerada pois procuramos linhas com índice > init_index.
    result = get_next_row_aftertruncated(table, init_index=0)
    assert result == -1


# Teste: Verifica o comportamento quando existem múltiplas linhas do tipo ROW após o init_index.


def test_get_next_row_aftertruncated_multiple():
    data = {
        "tipoDado": [
            ExtracTypeRow.TRUNCATED_DESCRIPTION,
            ExtracTypeRow.ROW,
            ExtracTypeRow.ROW,
            ExtracTypeRow.OTHERS,
        ]
    }
    table = DataFrame(data)
    # Com init_index=0, as linhas com índice > 0 são: 1, 2 e 3.
    # O primeiro que possui tipo ROW é a linha de índice 1.
    result = get_next_row_aftertruncated(table, init_index=0)
    assert result == 1


# Teste: Verifica o comportamento quando init_index é o último índice da tabela (ou não há linhas posteriores).


def test_get_next_row_aftertruncated_edge():
    data = {
        "tipoDado": [
            ExtracTypeRow.ROW,
            ExtracTypeRow.TRUNCATED_DESCRIPTION,
            ExtracTypeRow.ROW,
        ]
    }
    table = DataFrame(data)
    # Com init_index=2, não existem linhas com índice > 2, portanto deve retornar -1.
    result = get_next_row_aftertruncated(table, init_index=2)
    assert result == -1


# Teste: Verifica que a linha com índice igual ao init_index não é considerada.


def test_get_next_row_aftertruncated_exclusion():
    data = {"tipoDado": [ExtracTypeRow.ROW, ExtracTypeRow.ROW]}
    table = DataFrame(data)
    # Com init_index=0, a função ignora a linha de índice 0 e considera somente a linha de índice 1.
    result = get_next_row_aftertruncated(table, init_index=0)
    assert result == 1


# --- Testes para a função merge_truncated ---

# Cenário 1:
# - Duas linhas truncadas (um de descrição e outro de participante)
# - Em seguida, uma linha do tipo ROW que receberá os dados agregados.


def test_merge_truncated_basic():
    data = {
        "tipoDado": [
            ExtracTypeRow.TRUNCATED_DESCRIPTION,
            ExtracTypeRow.TRUNCATED_PARTICIPANT,
            ExtracTypeRow.ROW,
            ExtracTypeRow.ROW,  # Linha adicional para simular parada de busca
        ],
        "Data": ["", "", "2022-10-05", "2022-10-06"],
        "Descrição": ["Desc1", "Desc2", "Existing", "After"],
        "Participante": ["Part1", "Part2", "Existing", "After"],
        "Valor": ["", "", "", ""],
    }
    table = DataFrame(data)
    # Para init_index=0, get_next_row_aftertruncated retorna o primeiro ROW encontrado: índice 2.
    merged_table = merge_truncated(table, 0)
    expected_description = "Desc1 Desc2"
    expected_participant = "Part1 Part2"
    # Verifica se a linha de índice 2 teve os dados atualizados
    assert merged_table.loc[2, "Descrição"] == expected_description
    assert merged_table.loc[2, "Participante"] == expected_participant
    # As linhas truncadas (índices 0 e 1) devem ter sido removidas
    assert 0 not in merged_table.index
    assert 1 not in merged_table.index
    # A linha de índice 3 permanece inalterada
    assert merged_table.loc[3, "Descrição"] == "After"
    assert merged_table.loc[3, "Participante"] == "After"


# Cenário 2:
# - Tabela sem linhas truncadas (apenas linhas do tipo ROW).
# Nesse caso, a função deverá atualizar a primeira linha ROW encontrada (após init_index) com strings vazias,
# pois não há dados truncados para agregar.


def test_merge_truncated_sem_truncated():
    data = {
        "tipoDado": [ExtracTypeRow.ROW, ExtracTypeRow.ROW],
        "Data": ["2022-10-05", "2022-10-06"],
        "Descrição": ["Desc", "Desc2"],
        "Participante": ["Part", "Part2"],
        "Valor": ["", ""],
    }
    table = DataFrame(data)
    merged_table = merge_truncated(table, 0)
    # get_next_row_aftertruncated com init_index=0 ignora a linha de índice 0 e seleciona a linha de índice 1.
    # Como não há linhas truncadas, a concatenação resulta em strings vazias.
    assert merged_table.loc[1, "Descrição"] == ""
    assert merged_table.loc[1, "Participante"] == ""
    # A linha de índice 0 permanece inalterada.
    assert merged_table.loc[0, "Descrição"] == "Desc"
    assert merged_table.loc[0, "Participante"] == "Part"


# Cenário 3:
# - Três linhas truncadas que serão mescladas (limite de merged_rows é 3).


def test_merge_truncated_limite_merged_rows():
    data = {
        "tipoDado": [
            ExtracTypeRow.TRUNCATED_DESCRIPTION,
            ExtracTypeRow.TRUNCATED_PARTICIPANT,
            ExtracTypeRow.TRUNCATED_DESCRIPTION,
            ExtracTypeRow.ROW,
        ],
        "Data": ["", "", "", "2022-10-07"],
        "Descrição": ["A", "B", "C", "Existing"],
        "Participante": ["X", "Y", "Z", "Existing"],
        "Valor": ["", "", "", ""],
    }
    table = DataFrame(data)
    merged_table = merge_truncated(table, 0)
    expected_description = "A B C"
    expected_participant = "X Y Z"
    # A função deve identificar a linha de destino (primeira ROW após os truncados, índice 3)
    # e mesclar os dados das três linhas truncadas.
    assert merged_table.loc[3, "Descrição"] == expected_description
    assert merged_table.loc[3, "Participante"] == expected_participant
    # As linhas de índices 0, 1 e 2 devem ser removidas
    for idx in [0, 1, 2]:
        assert idx not in merged_table.index
