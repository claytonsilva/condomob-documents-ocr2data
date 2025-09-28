import csv
import re
from types import FunctionType

import pandas

regex_page = re.compile(r"^(page_)(\d+_)(\d+-\d+)(.csv)")
regex_old_unit_format = re.compile(r"^(Un. )(\d*-QD\d*-LT\d*)$")


def rename_unit(
    participante: str, matched_units: pandas.DataFrame | pandas.Series
) -> str:
    if participante:
        for el in matched_units["Participante"]:
            return el
    return participante  # short circuit to preserve original name if list is empty


def add_configurated_column(
    column_name: str,
    configuration_data: pandas.DataFrame | pandas.Series,
    *_,
) -> str:
    if not configuration_data.empty:
        for _, itRow in pandas.DataFrame(configuration_data).iterrows():
            return itRow[column_name]  # pyright: ignore
    return ""


# TODO develop better signature
def add_periodo_competencia_column(
    column_name: str,
    configuration_data: pandas.DataFrame | pandas.Series,
    file: str,
    regex: re.Pattern = regex_page,
) -> str:
    match = re.match(regex, file)
    if match:
        return match.group(3) if len(match.groups()) >= 3 else ""
    return ""


def extract_common_unit_information(
    participante: str, regex: re.Pattern
) -> str:
    match = regex.match(participante)
    if match:
        return match.group(2)
    return participante  # short circuit to match a 0 or 1 result in case of not unit participant


def transform_generated_analytical_data(
    csv_page_path: str,
    analytical_accounts_configuration_url: str,
    analytical_units_renamed_list_url: str,
):
    accounts_configuration: pandas.DataFrame = pandas.DataFrame()
    units_to_rename: pandas.DataFrame = pandas.DataFrame()

    if analytical_accounts_configuration_url:
        accounts_configuration = pandas.read_csv(
            analytical_accounts_configuration_url,
            dtype={
                "ContaContabil": str,
                "ContaContabilGrupo": str,
                "ContaContabilNormalizado": str,
            },
        )

    if analytical_units_renamed_list_url:
        units_to_rename = pandas.read_csv(analytical_units_renamed_list_url)

    if not accounts_configuration.empty or not units_to_rename.empty:
        csv_to_transform: pandas.DataFrame = pandas.read_csv(
            csv_page_path, dtype=str
        ).fillna("")
        table_output = csv_to_transform.copy()

        if not units_to_rename.empty:
            table_output.insert(
                0,
                "ParticipanteReview",
                table_output.apply(
                    lambda row: rename_unit(
                        row["Participante"],
                        # TODO filter units_to_rename for better performance
                        units_to_rename[
                            units_to_rename["Participante"].str.match(
                                ".*"
                                + extract_common_unit_information(
                                    row["Participante"],
                                    regex_old_unit_format,
                                )
                            )
                        ],
                    ),
                    axis=1,
                ),  # pyright: ignore
            )

            table_output.drop(columns=["Participante"], inplace=True)
            table_output.rename(
                columns={
                    "ParticipanteReview": "Participante",
                },
                inplace=True,
            )

        if not accounts_configuration.empty:
            for column_name in [
                "PeriodoPrestacaoContas",
                "ContaContabilGrupo",
                "ContaContabilGrupoDescritivo",
                "Natureza",
                "NaturezaDescritivo",
                "CompoeTaxa",
                "AcordadoAssembleia",
                "ContaContabilDescritivo",
                "ContaContabilNormalizado",
            ]:
                existed_columns = list(
                    csv_to_transform.columns.values
                )  # get from base dataframe
                if column_name in existed_columns:
                    table_output.drop(columns=[column_name], inplace=True)
                fnlambda: FunctionType = add_configurated_column
                if column_name == "PeriodoPrestacaoContas":
                    fnlambda = add_periodo_competencia_column
                table_output.insert(
                    0,
                    column_name,
                    table_output.apply(
                        lambda row: fnlambda(  # noqa -- ignoring not bind loop because this argument does not change in internal loop
                            column_name,  # noqa  -- ignoring not bind loop because this argument does not change in internal loop
                            accounts_configuration[
                                (
                                    row["ContaContabil"]
                                    == accounts_configuration["ContaContabil"]
                                )
                                | (
                                    row["ContaContabil"]
                                    == accounts_configuration[
                                        "ContaContabilNormalizado"
                                    ]
                                )
                            ],
                            row["file"],
                        ),
                        axis=1,
                    ),  # pyright: ignore
                )
            table_output.drop(columns=["ContaContabil"], inplace=True)
            table_output.rename(
                columns={
                    "ContaContabilNormalizado": "ContaContabil",
                },
                inplace=True,
            )

        # we dont move from input when occur transformation yet
        table_output.to_csv(
            csv_page_path,
            index=False,
            quoting=csv.QUOTE_ALL,
        )
