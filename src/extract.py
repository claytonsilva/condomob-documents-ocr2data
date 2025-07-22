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
