import csv
import os
import re
import shutil
from types import FunctionType

import pandas
from google.cloud import bigquery

from processors.llmwhisperer_analytical import (
    process_txt_file as process_txt_file_llmwhisperer,
)
from services.gcp import (
    clear_data_analytical_from_file,
    upload_csv_to_bigquery,
)
from services.llmwhisperer import (
    process_pdf_file as process_pdf_file_llmwhisperer,
)
from utils.constants import FileType
from utils.spliter import split_pdf_to_pages

regex_page = re.compile(r"^(page_)(\d+_)(\d+-\d+)(.csv)")
regex_old_unit_format = re.compile(r"^(Un. )(\d*-QD\d*-LT\d*)$")


def rename_unit(
    participante: str, matched_units: pandas.DataFrame | pandas.Series
) -> str:
    for el in matched_units["Participante"]:
        return el
    return participante  # short circuit to preserve original name if list is empty


def is_this_file_type(path: str, type: FileType) -> bool:
    """
    Returns True if the file at 'path' is of the given 'type', otherwise False.
    """
    _, ext = os.path.splitext(path)
    return ext.upper() == type.value.upper()


def extract_common_unit_information(
    participante: str, regex: re.Pattern
) -> str:
    match = regex.match(participante)
    if match:
        return match.group(2)
    return participante  # short circuit to match a 0 or 1 result in case of not unit participant


def addConfiguratedColumn(
    column_name: str,
    configuration_data: pandas.DataFrame | pandas.Series,
    *_,
) -> str:
    if not configuration_data.empty:
        for _, itRow in pandas.DataFrame(configuration_data).iterrows():
            return itRow[column_name]  # pyright: ignore
    return ""


# TODO develop better signature
def addPeriodoCompetenciaColumn(
    column_name: str,
    configuration_data: pandas.DataFrame | pandas.Series,
    file: str,
    regex: re.Pattern = regex_page,
) -> str:
    match = re.match(regex, file)
    if match:
        return match.group(3) if len(match.groups()) >= 3 else ""
    return ""


def reprocess(
    source_dir: str,
    output_dir: str,
    process_txt_file_fn: FunctionType | None,
    process_pdf_file_fn: FunctionType,
    file_type: FileType,
    analytical_accounts_configuration: str,
    analytical_units_renamed_list: str,
    upload: bool,
    client: bigquery.Client,
) -> None:
    files: list[str] = [
        os.path.join(source_dir, file)
        for file in os.listdir(source_dir)
        if os.path.isfile(os.path.join(source_dir, file))
        and is_this_file_type(file, file_type)
    ]
    accounts_configuration: pandas.DataFrame = pandas.DataFrame()
    units_to_rename: pandas.DataFrame = pandas.DataFrame()

    if analytical_accounts_configuration:
        accounts_configuration = pandas.read_csv(
            analytical_accounts_configuration, dtype={"ContaContabil": str}
        )

    if analytical_units_renamed_list:
        units_to_rename = pandas.read_csv(analytical_units_renamed_list)

    for i, page_path in enumerate(files):
        print(f"Processing file {i + 1} of {len(files)}: {page_path}")

        match file_type:
            case FileType.PDF:
                print(f"Converting {page_path} to text...")
                file_txt_path = process_pdf_file_fn(
                    page_path,
                    page_path.replace(
                        ".pdf",
                        ".txt"
                        if process_pdf_file_fn == process_pdf_file_llmwhisperer
                        else ".csv",
                    ),
                )
                shutil.move(
                    file_txt_path,
                    os.path.join(output_dir, os.path.basename(file_txt_path)),
                )
            case FileType.TXT:
                if process_txt_file_fn is not None:
                    print(f"Converting {page_path} to csv...")
                    file_csv_path = process_txt_file_llmwhisperer(page_path)
                    shutil.move(
                        file_csv_path,
                        os.path.join(
                            output_dir, os.path.basename(file_csv_path)
                        ),
                    )
            case FileType.CSV:
                if (
                    not accounts_configuration.empty
                    or not units_to_rename.empty
                ):
                    csv_to_transform: pandas.DataFrame = pandas.read_csv(
                        page_path, dtype=str
                    )
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
                                        units_to_rename[
                                            "Participante"
                                        ].str.match(
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

                        table_output.drop(
                            columns=["Participante"], inplace=True
                        )
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
                                table_output.drop(
                                    columns=[column_name], inplace=True
                                )
                            fnlambda: FunctionType = addConfiguratedColumn
                            if column_name == "PeriodoPrestacaoContas":
                                fnlambda = addPeriodoCompetenciaColumn
                            table_output.insert(
                                0,
                                column_name,
                                table_output.apply(
                                    lambda row: fnlambda(  # noqa -- ignoring not bind loop because this argument does not change in internal loop
                                        column_name,  # noqa  -- ignoring not bind loop because this argument does not change in internal loop
                                        accounts_configuration[
                                            (
                                                row["ContaContabil"]
                                                == accounts_configuration[
                                                    "ContaContabil"
                                                ]
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
                        table_output.drop(
                            columns=["ContaContabil"], inplace=True
                        )
                        table_output.rename(
                            columns={
                                "ContaContabilNormalizado": "ContaContabil",
                            },
                            inplace=True,
                        )
                    # we dont move from input when occur transformation yet
                    table_output.to_csv(
                        page_path,
                        index=False,
                        quoting=csv.QUOTE_ALL,
                    )

                if upload:
                    print(f"Deleting existing data from {page_path}")
                    clear_data_analytical_from_file(
                        client, os.path.basename(page_path)
                    )
                    print(f"Uploading {page_path} to BigQuery...")
                    upload_csv_to_bigquery(client, page_path)
                    print("Uploaded to BigQuery.")
                shutil.move(
                    page_path,
                    os.path.join(output_dir, os.path.basename(page_path)),
                )


def run(
    path: str,
    output_dir: str,
    start: int,
    end: int | None,
    upload: bool,
    # TODO remove option reprocess from run command, use only method reprocess for that
    reprocess: bool,
    processed_dir: str,
    process_txt_file_fn: FunctionType | None,
    process_pdf_file_fn: FunctionType,
    client: bigquery.Client,
) -> None:
    os.makedirs(processed_dir, exist_ok=True)

    pdf_pages_list = split_pdf_to_pages(
        input_pdf_path=path,
        output_dir=output_dir,
        start=start,
        end=end,
    )

    for i, page_path in enumerate(pdf_pages_list, start=1):
        print(f"Processing page {i} of {len(pdf_pages_list)}: {page_path}")

        file_txt_output = page_path.replace(".pdf", ".txt")
        file_txt_processed_output = os.path.join(
            processed_dir, os.path.basename(file_txt_output)
        )

        if not os.path.exists(file_txt_output) or reprocess:
            # restore the file if it was processed before
            if reprocess and os.path.exists(file_txt_processed_output):
                print(f"Revert {page_path} txt from processed dir...")
                shutil.move(file_txt_processed_output, file_txt_output)
            else:
                print(f"Converting {page_path} to text...")
                file_txt_output = process_pdf_file_fn(
                    page_path,
                    page_path.replace(
                        ".pdf",
                        ".txt"
                        if process_pdf_file_fn == process_pdf_file_llmwhisperer
                        else ".csv",
                    ),
                )
            shutil.move(
                page_path,
                os.path.join(processed_dir, os.path.basename(page_path)),
            )
        if file_txt_output != "":
            file_csv_output = page_path.replace(".pdf", ".csv")
            file_csv_processed_output = os.path.join(
                processed_dir, os.path.basename(file_csv_output)
            )
            if not os.path.exists(file_csv_output) or reprocess:
                if reprocess and os.path.exists(file_csv_processed_output):
                    print(f"Revert {page_path} csv from processed dir...")
                    shutil.move(
                        file_csv_processed_output,
                        file_csv_output,
                    )
                else:
                    if process_txt_file_fn is not None:
                        print(f"Converting {page_path} to csv...")
                        file_csv_output = process_txt_file_fn(file_txt_output)
                shutil.move(
                    file_txt_output,
                    file_txt_processed_output,
                )

            if (
                upload
                and not os.path.exists(file_csv_output)
                and not reprocess
            ):
                print("you need to reprocess the file to upload it")

            if upload and os.path.exists(file_csv_output):
                print(f"Uploading {file_csv_output} to BigQuery...")
                upload_csv_to_bigquery(client, file_csv_output)
                print("Uploaded to BigQuery.")
                shutil.move(
                    file_csv_output,
                    file_csv_processed_output,
                )
