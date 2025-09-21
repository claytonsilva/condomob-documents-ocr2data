import csv
import math
import os
import re
from collections import defaultdict
from io import StringIO

import pandas as pd

from utils.constants import COLLUMNS_ANALYTICAL as COLLUMNS
from utils.constants import FileType
from utils.extract_utils import (
    extract_group_from_contacontabilcompleto,
    validate,
)

pattern_account_grouped = re.compile(r"^(\d+[\.\d+]*)( - )(.*$)")
pattern_split = re.compile(
    r"^([ ]*[1-2][\.[0-9]+]* - .*$)\n\n", flags=re.MULTILINE
)
pattern_table_text = re.compile(r"(^\+-|^\|)", flags=re.MULTILINE)
pattern_table_separator = re.compile(r"^\+-", flags=re.MULTILINE)
pattern_total_cell = re.compile(r"^TOTAL", flags=re.MULTILINE)
pattern_untitled_table_mixed = re.compile(r"\n\n", flags=re.MULTILINE)


def is_this_file_type(path: str, type: FileType) -> bool:
    """
    Returns True if the file at 'path' is of the given 'type', otherwise False.
    """
    _, ext = os.path.splitext(path)
    return ext.upper() == type.value.upper()


def openFile(path: str) -> StringIO:
    """
    Abre o arquivo de texto e retorna seu conteúdo.
    """
    with open(path, encoding="utf-8") as f:
        return StringIO(f.read())


def split_blocks(stream: str):
    """
    Divide o conteúdo do arquivo em blocos separados por linhas em branco.
    """
    return [
        block for block in re.split(pattern_split, stream) if block.strip()
    ]


def clean_table_text(text: str) -> str:
    """
    Limpa o texto da tabela, removendo caracteres indesejados.
    """
    if re.match(pattern_table_text, text):
        return "\n".join(
            [
                block
                for block in re.split(r"\n", extract_untitled_table(text))
                if re.match(pattern_table_text, block)
            ]
        )
    return text


def concat_dataframe_cells(df: pd.DataFrame) -> pd.DataFrame:
    """
    Concatena as células de um DataFrame em uma única célula.
    """
    for line in df.index[2:]:
        drop_row = False
        for column in df.columns:
            if (
                isinstance(df.at[line, column], str)
                and str(df.at[line, column]).strip() != ""
                and not isinstance(df.at[line, "Data"], str)
                # TODO: Handle case where df.at[line, "Data"] is not numeric before calling math.isnan() to avoid TypeError.
                and math.isnan(df.at[line, "Data"])
            ):
                df.at[line - 1, column] = (
                    str(df.at[line - 1, column]).strip()
                    + " "
                    + str(df.at[line, column]).strip()
                )
                drop_row = True
        if drop_row:
            df.drop(line, inplace=True)
    return df


def extract_untitled_table(text: str) -> str:
    """
    Extrai a tabela sem título do texto, removendo os parágrafos adicionais.

    As tables vem com a estrutura da tabelas ASCII seguido de dois paragrafos,
    caso tenha uma tabela sem titulo, ela será detectada como uma tabela anexada à primeira e deve ser removida.
    então será aplicado inicialmente split para extrair somente a primeira tabela.
    """
    if re.search(pattern_untitled_table_mixed, text):
        return re.split(pattern_untitled_table_mixed, text)[0]
    return text


def drop_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove linhas inválidas do DataFrame, onde a coluna 'Data' não é uma string válida.
    """
    return df[
        df["Data"].apply(lambda x: isinstance(x, str) and validate(x.strip()))
    ]  # type: ignore


def strip_string_cells(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove espaços em branco no início e no final de todas as células do DataFrame.
    """
    for column in df.columns:
        if df[column].dtype == "object":
            df[column] = df[column].str.strip()
    return df


def strip_columns_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove espaços em branco no início e no final dos nomes das colunas do DataFrame.
    """
    df.columns = df.columns.str.strip()
    return df


def find_similar_columns(lst: list[str], pattern: re.Pattern) -> dict:
    """
    Encontra colunas similares em uma lista de strings e retorna um dicionário com as colunas agrupadas.
    """
    grouped_items = defaultdict(list)
    for item in lst:
        if re.search(pattern, item):
            prefix = re.split(pattern, item)[0]
            grouped_items[prefix].append(item)
    return grouped_items


def convert_list_to_dict(data: list) -> dict:
    """
    Converte uma lista de strings em um dicionário, onde a chave é o primeiro elemento
    e o valor é o restante da string.
    """
    return {
        data[i - 1].strip(): from_ascii_table_to_dataframe(line)
        for i, line in enumerate(data)
        if re.match(pattern_table_text, line)
    }


def get_first_not_null_value(
    df: pd.DataFrame, index: int, input_list: list[str]
):
    for col in input_list:
        value = df.at[index, col]
        if (
            value is not None or math.isnan(value) or value == ""
        ):  # Check for non-null value
            return value.strip() if isinstance(value, str) else value
    return None  # Return None if no non-null value is found


def clean_total_ascii_table(table: str) -> str:
    """
    Limpa a tabela ASCII removendo a linha de totalização.
    """
    lines = table.split("\n")
    cleaned_lines = [
        line for line in lines if not re.match(pattern_total_cell, line)
    ]
    return "\n".join(cleaned_lines) if cleaned_lines else ""


def from_ascii_table_to_dataframe(table: str) -> pd.DataFrame:
    """
    Converte uma tabela ASCII em um DataFrame do pandas.
    """
    lines = table.split("\n")
    lines_to_skip = [
        i
        for i, line in enumerate(lines)  # jump header line
        if re.match(pattern_table_separator, line)
    ]
    df = strip_columns_names(
        pd.read_csv(
            StringIO(table),
            sep="|",
            skipinitialspace=True,
            engine="python",
            skiprows=lines_to_skip,
            index_col=0,
            dtype=str,
        ).reset_index(drop=True)
    )
    df = drop_invalid_rows(df)  # type: ignore
    columns_from_txt: list[str] = df.columns.str.strip().to_list()[
        :-1
    ]  # remove last column because is aways empty
    # situações de falha de processamento que necessita de tratamento
    if columns_from_txt != COLLUMNS.to_list():
        # falha de processamento que gera coluna repetida
        if (
            len(
                list(
                    filter(
                        lambda el: re.search(r"( \.[0-9]+)$", el),
                        columns_from_txt,
                    )
                )
            )
            > 0
        ):
            print("Processing error: duplicate column title found., fixing...")
            repeated_columns = find_similar_columns(
                columns_from_txt, re.compile(r"( \.[0-9]+)$")
            )
            for key, value in repeated_columns.items():
                columns_not_empty = []
                for column in value:
                    if df[column].isna().all():  # type: ignore
                        columns_not_empty.append(column)
                for item in df[key].index:
                    if (
                        df.at[item, key] is None
                        or math.isnan(df.at[item, key])
                        or df.at[item, key] == ""
                    ):
                        df.at[item, key] = get_first_not_null_value(
                            df, item, columns_not_empty
                        )
                df.drop(columns=value, inplace=True)
                print(f"finished removing duplicate columns for {key}")
                columns_from_txt = df.columns.str.strip().to_list()[
                    :-1
                ]  # remove last column because is aways empty
        # falha de processamento quando funde duas colunas  Descrição e Participante
        if "Descrição Participante" in df.columns.str.strip():
            print(
                "Processing error: 'Descrição Participante' column found., fixing..."
            )
            df.rename(
                columns={
                    "Descrição Participante": "Descrição",
                },
                inplace=True,
            )
            df.insert(loc=2, column="Participante", value="")
            print("Fixed 'Descrição Participante' column.")
            columns_from_txt = df.columns.str.strip().to_list()[
                :-1
            ]  # remove last column because is aways empty

        # falha de processamento que gera coluna vazia no meio da tabela
        if (
            len(
                list(
                    filter(
                        lambda el: re.search(r"^Unnamed", el), columns_from_txt
                    )
                )
            )
            > 0
        ):
            print("Processing error: empty column title found., fixing...")
            if df["Valor"].isna().all():  # type: ignore
                print("Iniciando movimentação de dados para coluna correta")
                df.drop(columns=["Valor"], inplace=True)
                for index, column in reversed(
                    list(enumerate(columns_from_txt))
                ):  # type: ignore
                    if re.search(r"^Unnamed", column):
                        break
                    df.rename(
                        columns={
                            columns_from_txt[index - 1]: columns_from_txt[
                                index
                            ],
                        },
                        inplace=True,
                    )
                print("Colunas movimentadas com sucesso.. continuando...")
            else:
                print(
                    "Situação não tratada: coluna Valor preenchida, por enquanto deixa como está"
                )
            columns_from_txt = df.columns.str.strip().to_list()[
                :-1
            ]  # remove last column because is aways empty
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    df.columns = COLLUMNS
    return strip_string_cells(concat_dataframe_cells(df))  # type: ignore


def data_processing(data: dict, filename: str) -> pd.DataFrame:
    """
    Processa os dados do dicionário e retorna um DataFrame do pandas.
    """
    return_data = pd.DataFrame([], columns=COLLUMNS)
    for key, value in data.items():
        if isinstance(value, pd.DataFrame):
            inner_data = value.copy()
            inner_data["ContaContabilCompleto"] = key
            inner_data["ContaContabilDescritivo"] = (
                extract_group_from_contacontabilcompleto(
                    pattern_account_grouped, key, 3
                )
            )
            inner_data["ContaContabil"] = (
                extract_group_from_contacontabilcompleto(
                    pattern_account_grouped, key, 1
                )
            )
            inner_data["file"] = filename
            inner_data.drop(columns=["ContaContabilCompleto"], inplace=True)
            return_data = pd.concat(
                [return_data, inner_data], ignore_index=True
            )
    return_data["Valor"] = (
        return_data["Valor"]
        .replace(to_replace=r"\.", value="", regex=True)
        .replace(to_replace=r",", value=".", regex=True)
        .astype(pd.Float64Dtype())
    )
    return_data["ContaContabil"] = return_data["ContaContabil"].astype(str)
    return_data["Documento"] = return_data["Documento"].astype(str)
    return_data["Data"] = pd.to_datetime(
        return_data["Data"], format="%d/%m/%Y"
    )
    return_data.rename(
        columns={
            "Descrição": "Descricao",
        },
        inplace=True,
    )
    return_data.rename(
        columns={
            "Período": "Periodo",
        },
        inplace=True,
    )
    return_data.fillna("", inplace=True)
    return return_data


def process_txt_file(path: str):
    file_csv_output = path.replace(".txt", ".csv")
    blocks = split_blocks(openFile(path).getvalue())
    data_processing(
        convert_list_to_dict([clean_table_text(block) for block in blocks]),
        os.path.basename(file_csv_output),
    ).to_csv(
        file_csv_output,
        index=False,
        quoting=csv.QUOTE_NONNUMERIC,
    )
    return file_csv_output
