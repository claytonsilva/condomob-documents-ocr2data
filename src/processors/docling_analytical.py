"""
This module provides functionality to convert a PDF document,
this includes parsing the document, performing OCR using Tesseract and docling library,
and extracting tables from the document.
"""

import logging
import re

import pandas as pd
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel.accelerator_options import (
    AcceleratorDevice,
    AcceleratorOptions,
)
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    TableFormerMode,
    TesseractCliOcrOptions,
)
from docling.document_converter import (
    DocumentConverter,
    ImageFormatOption,
)
from pandas import DataFrame, Series

from utils.constants import COLUMNS_ANALYTICAL as COLUMNS
from utils.constants import ExtractTypeRow
from utils.extract_utils import (
    extract_group_from_contacontabilcompleto,
    validate,
)

pattern = re.compile(r"^(\d+\.\d[0-9.]*)( - )(.*$)")


def get_current_title(table: DataFrame, row: Series | DataFrame):
    for _, itRow in table[row.name :: -1].iterrows():
        type_row = itRow["tipoDado"]
        if type_row == ExtractTypeRow.TITLE:
            return itRow["Data"]
    return None


def identify_row(line: Series) -> ExtractTypeRow | None:
    """
    Identifies the type of row based on the content of the 'Data' column.
    Returns an instance of ExtractTypeRow enum.
    """
    first_column = line.loc["Data"].strip()

    if first_column == "Data":
        return ExtractTypeRow.HEADERS
    elif re.match(r"^TOTAL: \d+\.\d+.*", first_column):
        return ExtractTypeRow.TOTAL
    elif re.match(r"^(\d+\.\d[0-9.]*)( - )(.*$)", first_column):
        return ExtractTypeRow.TITLE
    elif validate(first_column):
        return ExtractTypeRow.ROW
    else:
        return ExtractTypeRow.OTHERS


def process_pdf_file(input_path: str, output_path: str):
    """
    Main function to convert a PDF document and extract tables.
    It uses the Docling library to parse the PDF and Tesseract for OCR.
    The extracted tables are processed to identify and categorize rows,
    and then saved as CSV and HTML files.
    """

    logging.basicConfig(level=logging.INFO)

    # Docling Parse with Tesseract
    #    ----------------------
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = True
    pipeline_options.do_table_structure = True
    pipeline_options.table_structure_options.do_cell_matching = True
    pipeline_options.ocr_options = TesseractCliOcrOptions(
        lang=["lat", "por", "Latin"],
    )
    pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE

    pipeline_options.accelerator_options = AcceleratorOptions(
        num_threads=4, device=AcceleratorDevice.AUTO
    )
    doc_converter = DocumentConverter(
        format_options={
            InputFormat.IMAGE: ImageFormatOption(
                pipeline_options=pipeline_options,
                backend=PyPdfiumDocumentBackend,
            )
        }
    )

    conv_res = doc_converter.convert(input_path)

    # Export tables
    for table_ix, table in enumerate(conv_res.document.tables):
        table_df: pd.DataFrame = table.export_to_dataframe()
        print(f"## Table {table_ix}")
        # Iremos fazer em cada tabela duas percorridas de loop
        table_output = table_df.copy()
        table_output.columns = COLUMNS
        # inserindo a colunas já com os tipos definidos
        table_output.insert(
            0,
            "tipoDado",
            table_output.apply(identify_row, axis=1),  # pyright: ignore
        )
        table_output.insert(
            0,
            "ContaContabilCompleto",
            table_output.apply(
                lambda row: get_current_title(table_output, row),  # noqa
                axis=1,
            ),  # pyright: ignore
        )
        # remover dados que não serão mais usados
        table_output.drop(
            table_output[table_output["tipoDado"] != ExtractTypeRow.ROW].index,  # pyright: ignore
            inplace=True,
        )  # pyright: ignore
        # no final insere as colunas sumárias da tabela
        table_output.insert(
            0,
            "ContaContabilDescritivo",
            table_output.apply(
                lambda row: extract_group_from_contacontabilcompleto(
                    pattern, row["ContaContabilCompleto"], 3
                ),
                axis=1,
            ),  # pyright: ignore
        )
        table_output.insert(
            0,
            "ContaContabil",
            table_output.apply(
                lambda row: extract_group_from_contacontabilcompleto(
                    pattern, row["ContaContabilCompleto"], 1
                ),
                axis=1,
            ),  # pyright: ignore
        )
        table_output.drop(
            columns=["tipoDado", "ContaContabilCompleto"], inplace=True
        )

        table_output.to_csv(output_path)
        return output_path
