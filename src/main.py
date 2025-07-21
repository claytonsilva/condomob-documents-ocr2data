import logging
import re
import time
from pathlib import Path

import pandas as pd
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    TesseractCliOcrOptions,
)
from docling.document_converter import DocumentConverter, PdfFormatOption

from constants import COLLUMNS, ExtracTypeRow
from extract import (
    get_current_title,
    identify_row,
)

_log = logging.getLogger(__name__)


def main():
    logging.basicConfig(level=logging.INFO)

    data_folder = Path(__file__).parent.parent / "files"
    input_doc_path = data_folder / "2024-02-analitico-p60-teste-docling.pdf"
    output_dir = Path(__file__).parent.parent / "output"

    # Docling Parse with Tesseract
    #    ----------------------
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = True
    pipeline_options.do_table_structure = True
    pipeline_options.table_structure_options.do_cell_matching = True
    pipeline_options.ocr_options = TesseractCliOcrOptions(
        lang=["lat", "por", "Latin"]
    )

    doc_converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )

    start_time = time.time()

    conv_res = doc_converter.convert(input_doc_path)

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
            0, "tipoDado", table_output.apply(identify_row, axis=1)
        )
        table_output.insert(
            0,
            "ContaContabilCompleto",
            table_output.apply(
                lambda row: get_current_title(table_output, row),  # noqa
                axis=1,
            ),
        )
        # remover dados que não serão mais usados
        table_output.drop(
            table_output[table_output["tipoDado"] != ExtracTypeRow.ROW].index,
            inplace=True,
        )
        # no final insere as colunas sumárias da tabela
        table_output.insert(
            0,
            "ContaContabilDescritivo",
            table_output.apply(
                lambda row: re.match(
                    r"^(\d+\.\d[0-9.]*)( - )(.*$)",
                    row["ContaContabilCompleto"],
                ).group(3),
                axis=1,
            ),
        )
        table_output.insert(
            0,
            "ContaContabil",
            table_output.apply(
                lambda row: re.match(
                    r"^(\d+\.\d[0-9.]*)( - )(.*$)",
                    row["ContaContabilCompleto"],
                ).group(1),
                axis=1,
            ),
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

        # Save the table as html
        element_html_filename = (
            output_dir / f"{doc_filename}-table-{table_ix + 1}.html"
        )
        _log.info(f"Saving HTML table to {element_html_filename}")
        with element_html_filename.open("w") as fp:
            fp.write(table.export_to_html(doc=conv_res.document))

    end_time = time.time() - start_time

    _log.info(
        f"Document converted and tables exported in {end_time:.2f} seconds."
    )


if __name__ == "__main__":
    main()
