import os
import tempfile

from pypdf import PdfWriter

from spliter import split_pdf_to_pages


def create_sample_pdf(path, num_pages=3):
    writer = PdfWriter()
    for _ in range(num_pages):
        writer.add_blank_page(width=72, height=72)
    with open(path, "wb") as f:
        writer.write(f)


def test_split_pdf_to_pages_creates_individual_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_pdf = os.path.join(tmpdir, "sample.pdf")
        output_dir = os.path.join(tmpdir, "pages")
        create_sample_pdf(input_pdf, num_pages=4)
        split_pdf_to_pages(input_pdf, output_dir)
        files = sorted(os.listdir(output_dir))
        assert len(files) == 4
        for i, filename in enumerate(files, start=1):
            assert filename.startswith(f"page_{i}_sample.pdf")
            assert filename.endswith(".pdf")
            # Check that each file is a valid PDF with one page
            from pypdf import PdfReader

            page_path = os.path.join(output_dir, filename)
            reader = PdfReader(page_path)
            assert len(reader.pages) == 1


def test_split_pdf_to_pages_output_dir_created():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_pdf = os.path.join(tmpdir, "input.pdf")
        output_dir = os.path.join(tmpdir, "new_pages")
        create_sample_pdf(input_pdf, num_pages=2)
        assert not os.path.exists(output_dir)
        split_pdf_to_pages(input_pdf, output_dir)
        assert os.path.exists(output_dir)
        assert len(os.listdir(output_dir)) == 2


def test_split_pdf_to_pages_empty_pdf():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_pdf = os.path.join(tmpdir, "empty.pdf")
        output_dir = os.path.join(tmpdir, "empty_pages")
        # Create an empty PDF
        writer = PdfWriter()
        with open(input_pdf, "wb") as f:
            writer.write(f)
        split_pdf_to_pages(input_pdf, output_dir)
        assert os.path.exists(output_dir)
        assert len(os.listdir(output_dir)) == 0
