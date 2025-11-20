import os

import typer
from dotenv import load_dotenv
from google.cloud import bigquery

from analytical import reprocess as reprocess_analytical_import
from analytical import run as run_analytical_import
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
from utils.merger import merge_document
from utils.spliter import split_pdf_to_pages as split_pdf_import

# Create Typer apps
app = typer.Typer()
analytical_app = typer.Typer()
spliter_app = typer.Typer()
merger_app = typer.Typer()
app.add_typer(analytical_app, name="analytical", help="Analytical commands")
app.add_typer(spliter_app, name="spliter", help="Splitting commands")
app.add_typer(merger_app, name="merger", help="Merge commands")


def run_analytical_function(
    path: str,
    output_dir: str,
    dataset_id: str,
    table_id: str,
    client: bigquery.Client,
    start: int = 1,
    end: int | None = None,
    reprocess: bool = False,
    processed_dir: str = "",
    upload: bool = False,
    method: MethodType = MethodType.llmwhisperer,
):
    return run_analytical_import(
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
        dataset_id=dataset_id,
        table_id=table_id,
    )


def reprocess_analytical_function(
    path: str,
    output_dir: str,
    dataset_id: str,
    table_id: str,
    client: bigquery.Client,
    method: MethodType = MethodType.llmwhisperer,
    file_type: FileType = FileType.TXT,
    upload: bool = False,
):
    return reprocess_analytical_import(
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
        dataset_id=dataset_id,
        table_id=table_id,
    )


def split_pdf_function(
    path: str,
    output_dir: str = "output",
    start: int = 1,
    end: int | None = None,
):
    """
    Split a PDF file into individual pages and save them to the output directory.

    This is a wrapper for split_pdf_to_pages, extracted to separate business logic from CLI commands.

    Args:
        path (str): Path to the input PDF file.
        output_dir (str): Directory where the split pages will be saved.
        start (int): The first page to split (1-based index).
        end (int, optional): The last page to split. If None, splits to the last page.

    Returns:
        List[str]: List of file paths to the split PDF pages.
    """
    return split_pdf_import(path, output_dir, start, end)


# Typer command decorators that call the functions
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
    return run_analytical_function(
        path=path,
        output_dir=output_dir,
        start=start,
        end=end,
        reprocess=reprocess,
        processed_dir=processed_dir,
        upload=upload,
        method=method,
        dataset_id=os.environ["GOOGLE_CLOUD_BIGQUERY_DATASET_ID"],
        table_id=os.environ["GOOGLE_CLOUD_BIGQUERY_TABLE_ID_ANALYTICAL"],
        client=bigquery.Client(),
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
    return reprocess_analytical_function(
        path=path,
        output_dir=output_dir,
        method=method,
        file_type=file_type,
        upload=upload,
        dataset_id=os.environ["GOOGLE_CLOUD_BIGQUERY_DATASET_ID"],
        table_id=os.environ["GOOGLE_CLOUD_BIGQUERY_TABLE_ID_ANALYTICAL"],
        client=bigquery.Client(),
    )


@spliter_app.command(name="run", help="Split a PDF file into individual pages")
def run_split(
    path: str,
    output_dir: str = "output",
    start: int = 1,
    end: int | None = None,
):
    return split_pdf_function(
        path=path,
        output_dir=output_dir,
        start=start,
        end=end,
    )


@merger_app.command(
    name="run", help="Merge a Folder with csv's files in a single csv"
)
def run_merger(
    path_dir: str = os.path.join(os.getcwd(), "output"),
    output: str = os.path.join(os.getcwd(), "processed", "merged.csv"),
):
    merge_document(path_dir, output)


if __name__ == "__main__":
    load_dotenv()  # Load environment variables from .env file
    app()
