import math
import os
import re
from io import StringIO

import pandas as pd

from constants import COLLUMNS
from extract import extract_group_from_contacontabilcompleto, validate
from llmwhisperer import process_file
from spliter import split_pdf_to_pages

pattern_account = re.compile(r"(^[1-2]\.[0-9]+ - .*$)")
pattern_account_grouped = re.compile(r"^(\d+\.\d[0-9]+)( - )(.*$)")
pattern_split = re.compile(
    r"^([ ]*[1-2]\.[0-9]+ - .*$)\n\n", flags=re.MULTILINE
)
pattern_table_text = re.compile(r"(^\+-|^\|)", flags=re.MULTILINE)
pattern_table_separator = re.compile(r"^\+-", flags=re.MULTILINE)
pattern_total_cell = re.compile(r"^TOTAL", flags=re.MULTILINE)


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
                for block in re.split(r"\n", text)
                if re.match(pattern_table_text, block)
            ]
        )
    return text


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


def concat_dataframe_cells(df: pd.DataFrame) -> pd.DataFrame:
    """
    Concatena as células de um DataFrame em uma única célula.
    """
    for line in df.index[2:]:
        drop_row = False
        for column in df.columns:
            if (
                isinstance(df.at[line, column], str)
                and df.at[line, column].strip() != ""
                and not isinstance(df.at[line, "Data"], str)
                and math.isnan(df.at[line, "Data"])
            ):
                df.at[line - 1, column] = (
                    df.at[line - 1, column].strip()
                    + " "
                    + df.at[line, column].strip()
                )
                drop_row = True
        if drop_row:
            df.drop(line, inplace=True)
    return df


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
    df = pd.read_csv(
        StringIO(table),
        sep="|",
        skipinitialspace=True,
        engine="python",
        skiprows=lines_to_skip,
        index_col=0,
        dtype=str,
    ).reset_index(drop=True)
    if "Descrição Participante" in df.columns.str.strip():
        df.rename(
            columns={
                "Descrição Participante": "Descrição",
            },
            inplace=True,
        )
        df.insert(loc=2, column="Participante", value="")
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    df.columns = COLLUMNS
    return strip_string_cells(
        concat_dataframe_cells(drop_invalid_rows(df))  # type: ignore
    )


def data_processing(data: dict) -> pd.DataFrame:
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
    return_data["Data"] = pd.to_datetime(
        return_data["Data"], format="%d/%m/%Y"
    )
    return_data.fillna("", inplace=True)
    return return_data


def run(
    path: str,
    output_dir: str,
    start: int = 0,
    end: int | None = None,
) -> None:
    pdf_pages_list = split_pdf_to_pages(
        input_pdf_path=path,
        output_dir=output_dir,
        start=start,
        end=end,
    )

    for i, page_path in enumerate(pdf_pages_list, start=1):
        print(f"Processing page {i} of {len(pdf_pages_list)}: {page_path}")

        file_txt_output = page_path.replace(".pdf", ".txt")
        if not os.path.exists(file_txt_output):
            print(f"Converting {page_path} to text...")
            file_txt_output = process_file(page_path)

        if file_txt_output != "":
            file_csv_output = page_path.replace(".pdf", ".csv")
            if not os.path.exists(file_csv_output):
                blocks = split_blocks(openFile(file_txt_output).getvalue())
                data_processing(
                    convert_list_to_dict(
                        [clean_table_text(block) for block in blocks]
                    )
                ).to_csv(file_csv_output, index=False)
