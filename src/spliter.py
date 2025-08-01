import os

from pypdf import PdfReader, PdfWriter


def generate_page_filenames(index: int, input_pdf_filename: str) -> str:
    """
    Generates a list of filenames for each page in a PDF file.
    Args:
        index (int): The page number (1-based index).
        input_pdf_filename (str): The name of the input PDF file.
    Returns:
        str: A string representing the filename for a specific page.
    """
    return f"page_{index}_{input_pdf_filename}"


def split_pdf_to_pages(
    input_pdf_path: str,
    output_dir: str,
    start: int = 0,
    end: int | None = None,
) -> list[str]:
    """
    Splits a PDF file into individual pages and saves each page as a separate PDF file.

    Args:
        input_pdf_path (str): Path to the input PDF file.
        output_dir (str): Directory where the split PDF pages will be saved.
        start (int): The starting page number (1-based index) to split from.
        end (int | None): The ending page number (1-based index) to split to. If None, splits to the last page.
    Returns:
        None
    """
    output: list[str] = []
    os.makedirs(output_dir, exist_ok=True)
    reader = PdfReader(input_pdf_path)
    input_pdf_filename = os.path.basename(input_pdf_path)
    pages = (
        range(start, end + 1)
        if end is not None
        else range(start + 1, len(reader.pages) + 1)
    )
    _end: int = end + 1 if end is not None else len(reader.pages)
    for i, page in enumerate(
        reader.pages[start:_end],
        start=1,
    ):
        output_path = os.path.join(
            output_dir,
            generate_page_filenames(pages[i - 1], input_pdf_filename),
        )
        if not os.path.exists(output_path):
            print(
                f"Generating single page {pages[i - 1]} for {input_pdf_filename}"
            )
            writer = PdfWriter()
            writer.add_page(page)
            with open(output_path, "wb") as out_file:
                writer.write(out_file)
        output.append(output_path)
    return output
