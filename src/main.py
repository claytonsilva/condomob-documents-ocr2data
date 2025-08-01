import typer

from analytical import run as run_analytical
from analytical_v2 import run as run_analytical_v2
from spliter import split_pdf_to_pages

app = typer.Typer()
analytical_app = typer.Typer()
spliter_app = typer.Typer()
app.add_typer(analytical_app, name="analytical", help="Analytical commands")
app.add_typer(spliter_app, name="spliter", help="Splitting commands")


@analytical_app.command(
    help="Run the analytical extraction process on a PDF file"
)
def run(path: str):
    run_analytical(path)


@analytical_app.command(
    help="Run the analytical extraction process on a PDF file with a different approach"
)
def run_v2(
    path: str,
    output_dir: str = "output",
    start: int = 1,
    end: int | None = None,
):
    return run_analytical_v2(
        path,
        output_dir,
        start,
        end,
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
    app()
