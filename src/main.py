import os

import typer
from dotenv import load_dotenv
from google.cloud import bigquery

from analytical import reprocess as reprocess_analytical
from analytical import run as run_analytical
from processors.docling_analytical import (
    process_pdf_file as process_pdf_file_docling,
)
from processors.llmwhisperer_analytical import (
    process_txt_file as process_txt_file_llmwhisperer,
)
from services.llmwhisperer import (
    process_pdf_file as process_pdf_file_llmwhisperer,
)
from utils.constants import FileType, MethodType
from utils.spliter import split_pdf_to_pages

app = typer.Typer()
analytical_app = typer.Typer()
spliter_app = typer.Typer()
app.add_typer(analytical_app, name="analytical", help="Analytical commands")
app.add_typer(spliter_app, name="spliter", help="Splitting commands")


@analytical_app.command(
    help="Run the analytical extraction process on a PDF file with a different approach"
)
def run(
    path: str,
    output_dir: str = os.path.join(os.getcwd(), "output"),
    start: int = 1,
    end: int | None = None,
    upload: bool = False,
    processed_dir: str = os.path.join(os.getcwd(), "processed"),
    reprocess: bool = False,
    method: MethodType = MethodType.llmwhisperer,
):
    client = bigquery.Client("realpark-dev")

    return run_analytical(
        path,
        output_dir,
        start,
        end,
        reprocess=reprocess,
        processed_dir=processed_dir,
        process_pdf_file_fn=process_pdf_file_llmwhisperer
        if method == MethodType.llmwhisperer
        else process_pdf_file_docling,
        process_txt_file_fn=process_txt_file_llmwhisperer
        if method == MethodType.llmwhisperer
        else None,
        analytical_accounts_configuration=os.environ[
            "GOOGLE_SHEET_ACCOUNT_PLAN_ANALYTICAL_URL"
        ],
        analytical_units_renamed_list=os.environ[
            "GOOGLE_SHEET_RENAMED_UNITS_ANALYTICAL_URL"
        ],
        upload=upload,
        client=client,
    )


@analytical_app.command(
    help="Run the analytical extraction process on a PDF file with a different approach"
)
def reprocess(
    path: str,
    output_dir: str = os.path.join(os.getcwd(), "output"),
    method: MethodType = MethodType.llmwhisperer,
    file_type: FileType = FileType.TXT,
    upload: bool = False,
):
    client = bigquery.Client(
        "realpark-dev"
    )  # TODO create env variable to store project info

    return reprocess_analytical(
        path,
        output_dir,
        process_pdf_file_fn=process_pdf_file_llmwhisperer
        if method == MethodType.llmwhisperer
        else process_pdf_file_docling,
        process_txt_file_fn=process_txt_file_llmwhisperer
        if method == MethodType.llmwhisperer
        else None,
        file_type=file_type,
        analytical_accounts_configuration=os.environ[
            "GOOGLE_SHEET_ACCOUNT_PLAN_ANALYTICAL_URL"
        ],
        analytical_units_renamed_list=os.environ[
            "GOOGLE_SHEET_RENAMED_UNITS_ANALYTICAL_URL"
        ],
        upload=upload,
        client=client,
    )


@spliter_app.command(name="run", help="Split a PDF file into individual pages")
def run_split(
    path: str,
    output_dir: str = "output",
    start: int = 1,
    end: int | None = None,
):
    split_pdf_to_pages(path, output_dir, start, end)


if __name__ == "__main__":
    load_dotenv()  # Load environment variables from .env file
    app()
