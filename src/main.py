import typer
from dotenv import load_dotenv

from analytical import run as run_analytical
from processors.docling_analytical import process_file as process_file_docling
from processors.llmwhisperer import process_file as processs_file_llmwhisperer
from utils.constants import MethodType
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
    output_dir: str = "output",
    start: int = 1,
    end: int | None = None,
    upload: bool = False,
    processed_dir: str = "processed",
    reprocess: bool = False,
    method: MethodType = MethodType.llmwhisperer,
):
    return run_analytical(
        path,
        output_dir,
        start,
        end,
        upload=upload,
        reprocess=reprocess,
        processed_dir=processed_dir,
        process_file_fn=processs_file_llmwhisperer
        if method == MethodType.llmwhisperer
        else process_file_docling,
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
