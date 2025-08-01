"""
This module provides functionality to convert a PDF document,
this includes parsing the document, performing OCR using Tesseract and docling library,
and extracting tables from the document.
"""

import logging
import re
import time
from pathlib import Path

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

from constants import COLLUMNS, ExtracTypeRow
from extract import (
    extract_group_from_contacontabilcompleto,
    get_current_title,
    identify_row,
)

_log = logging.getLogger(__name__)
pattern = re.compile(r"^(\d+\.\d[0-9.]*)( - )(.*$)")


def run(path: str):
    """
    Main function to convert a PDF document and extract tables.
    It uses the Docling library to parse the PDF and Tesseract for OCR.
    The extracted tables are processed to identify and categorize rows,
    and then saved as CSV and HTML files.
    """

    logging.basicConfig(level=logging.INFO)

    output_dir = Path(__file__).parent.parent / "output"

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

    start_time = time.time()

    conv_res = doc_converter.convert(path)

    output_dir.mkdir(parents=True, exist_ok=True)

    doc_filename = conv_res.input.file.stem

    # Export tables
    for table_ix, table in enumerate(conv_res.document.tables):
        table_df: pd.DataFrame = table.export_to_dataframe()
        print(f"## Table {table_ix}")
        # Iremos fazer em cada tabela duas percorridas de loop
        table_output = table_df.copy()
        table_output.columns = COLLUMNS
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
            table_output[table_output["tipoDado"] != ExtracTypeRow.ROW].index,  # pyright: ignore
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

        print(table_output.to_markdown())

        # Save the table as csv
        element_csv_filename = (
            output_dir / f"{doc_filename}-table-{table_ix + 1}.csv"
        )
        _log.info(f"Saving CSV table to {element_csv_filename}")
        table_output.to_csv(element_csv_filename)

    end_time = time.time() - start_time

    _log.info(
        f"Document converted and tables exported in {end_time:.2f} seconds."
    )
