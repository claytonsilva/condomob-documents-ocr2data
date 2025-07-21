import re
from datetime import datetime

from pandas import DataFrame, Series

from constants import ExtracTypeRow


def get_current_title(table: DataFrame, row: Series) -> Series:
    for _, itRow in table[row.name :: -1].iterrows():
        type_row = itRow["tipoDado"]
        if type_row == ExtracTypeRow.TITLE:
            return itRow["Data"]
    return None


# identifica a primeira linha com truncamento


def has_truncated_rows(table: DataFrame, init_index: int) -> int:
    truncated_row_index = -1
    for index, row in table[table.index >= init_index].iterrows():
        type_row = row["tipoDado"]
        if (
            type_row == ExtracTypeRow.TRUNCATED_DESCRIPTION
            or type_row == ExtracTypeRow.TRUNCATED_PARTICIPANT
        ):
            truncated_row_index = index
            break
    return truncated_row_index


def validate(date_text):
    try:
        if date_text != datetime.strptime(date_text, "%d/%m/%Y").strftime(
            "%d/%m/%Y"
        ):
            raise ValueError
        return True
    except ValueError:
        return False


def identify_row(line: Series) -> ExtracTypeRow | None:
    first_collumn = line.loc["Data"].strip()

    if first_collumn == "Data":
        return ExtracTypeRow.HEADERS
    elif re.match(r"^TOTAL: \d+\.\d+.*", first_collumn):
        return ExtracTypeRow.TOTAL
    elif re.match(r"^(\d+\.\d[0-9.]*)( - )(.*$)", first_collumn):
        return ExtracTypeRow.TITLE
    elif validate(first_collumn):
        return ExtracTypeRow.ROW
    else:
        return ExtracTypeRow.OTHERS


def append_data(current_data: str, new_data: str) -> str:
    return " ".join([current_data, new_data]).strip()


def get_next_row_aftertruncated(table: DataFrame, init_index: int) -> int:
    next_row_index = -1
    for index, row in table[table.index > init_index].iterrows():
        type_row = row["tipoDado"]
        if type_row == ExtracTypeRow.ROW:
            next_row_index = index
            break
    return next_row_index


def merge_truncated(table: DataFrame, init_index: int) -> DataFrame:
    # identificar a linha imediata que contém a descrição truncada
    rows_to_drop = []
    description = ""
    participant = ""
    description_index = "Descrição"
    participant_index = "Participante"
    merged_rows = 0
    row_to_append_index = get_next_row_aftertruncated(table, init_index)
    row_to_stop_search_index = get_next_row_aftertruncated(
        table, row_to_append_index
    )
    for index, row in table[
        (table.index >= init_index)
        & (
            (table.index < row_to_stop_search_index)
            & (row_to_stop_search_index >= 0)
            | (row_to_stop_search_index < 0)
        )
        & (
            (table["tipoDado"] == ExtracTypeRow.TRUNCATED_DESCRIPTION)
            | (table["tipoDado"] == ExtracTypeRow.TRUNCATED_PARTICIPANT)
            # | ((table['tipoDado'] == ExtracTypeRow.ROW) &
            #     ((table['Descrição'] == '') |
            #      (table['Participante'] == ''))
            #  )
        )
    ].iterrows():
        type_row = row["tipoDado"]
        next_row = (
            Series([])
            if len(table[table.index > index].index) == 0
            else table.loc[table[table.index > index].index[0]]
        )
        prev_row = (
            Series([])
            if len(table[table.index < index].index) == 0
            else table.loc[table[table.index < index].index[-1]]
        )

        if (
            type_row == ExtracTypeRow.TRUNCATED_DESCRIPTION
            or type_row == ExtracTypeRow.TRUNCATED_PARTICIPANT
            or index == row_to_append_index
        ):
            if (
                type_row == ExtracTypeRow.TRUNCATED_DESCRIPTION
                or type_row == ExtracTypeRow.TRUNCATED_PARTICIPANT
            ):
                rows_to_drop.append(index)

            merged_rows += 1
            description = append_data(
                description, row.loc[description_index].strip()
            )
            participant = append_data(
                participant, row.loc[participant_index].strip()
            )

        # checagem extra
        # temos dois casos comuns de multiplas linhas truncadas, onde teremos
        # 1) 3 linhas truncadas inicio/meio com os dados/fim
        # 1) 2 linhas truncadas inicio/fim com os dados
        # em ambos podem acontecer dados intercalados por outros tipos de linhas
        # quando trunca participante ele fica separado em uma linha única
        if merged_rows >= 3:
            break
        # quando trunca descrição segue o baile
        elif (
            index == row_to_append_index
            and prev_row["tipoDado"] != next_row["tipoDado"]
        ):
            break

    # out of the loop return merged data
    table_output = table.copy()
    table_output.at[row_to_append_index, description_index] = description
    table_output.at[row_to_append_index, participant_index] = participant
    table_output = table_output.drop(rows_to_drop)
    return table_output
