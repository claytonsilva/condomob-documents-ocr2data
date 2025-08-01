import re
from datetime import datetime

from pandas import DataFrame, Series

from constants import ExtracTypeRow

pattern_data = re.compile(
    r"^(?:(?:31(\/|-|\.)(?:0?[13578]|1[02]))\1|(?:(?:29|30)(\/|-|\.)(?:0?[13-9]|1[0-2])\2))(?:(?:1[6-9]|[2-9]\d)?\d{2})$|^(?:29(\/|-|\.)0?2\3(?:(?:(?:1[6-9]|[2-9]\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]|[3579][26])00))))$|^(?:0?[1-9]|1\d|2[0-8])(\/|-|\.)(?:(?:0?[1-9])|(?:1[0-2]))\4(?:(?:1[6-9]|[2-9]\d)?\d{2})$"
)


def get_current_title(table: DataFrame, row: Series):
    for _, itRow in table[row.name :: -1].iterrows():
        type_row = itRow["tipoDado"]
        if type_row == ExtracTypeRow.TITLE:
            return itRow["Data"]
    return None


def extract_group_from_contacontabilcompleto(
    pattern: re.Pattern, conta_contabil_completo: str, group: int
) -> str | None:
    """
    Extracts a specific group from the 'ContaContabilCompleto' string.
    The group is determined by the number of dots in the string.
    """
    match = re.match(
        pattern,
        conta_contabil_completo,
    )
    if match:
        return match.group(group) if group <= len(match.groups()) else None
    return None


def validate(date_text):
    """
    Validates if the date_text is in the format "dd/mm/yyyy".
    Returns True if valid, False otherwise.
    """
    try:
        if re.match(pattern_data, date_text) is None:
            return False
        if date_text != datetime.strptime(date_text, "%d/%m/%Y").strftime(
            "%d/%m/%Y"
        ):
            raise ValueError
        return True
    except ValueError:
        return False


def identify_row(line: Series) -> ExtracTypeRow | None:
    """
    Identifies the type of row based on the content of the 'Data' column.
    Returns an instance of ExtracTypeRow enum.
    """
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
