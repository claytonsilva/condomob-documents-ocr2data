import os
import shutil
from types import FunctionType

from processors.llmwhisperer_analytical import (
    process_txt_file as process_txt_file_llmwhisperer,
)
from services.gcp import upload_csv_to_bigquery
from services.llmwhisperer import (
    process_pdf_file as process_pdf_file_llmwhisperer,
)
from utils.constants import FileType
from utils.spliter import split_pdf_to_pages


def is_this_file_type(path: str, type: FileType) -> bool:
    """
    Returns True if the file at 'path' is of the given 'type', otherwise False.
    """
    _, ext = os.path.splitext(path)
    return ext.upper() == type.value.upper()


def reprocess(
    source_dir: str,
    output_dir: str,
    process_txt_file_fn: FunctionType | None,
    process_pdf_file_fn: FunctionType,
    file_type: FileType,
) -> None:
    files: list[str] = [
        os.path.join(source_dir, file)
        for file in os.listdir(source_dir)
        if os.path.isfile(os.path.join(source_dir, file))
        and is_this_file_type(file, file_type)
    ]

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
                print(f"Uploading {page_path} to BigQuery...")
                upload_csv_to_bigquery(page_path)
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
    reprocess: bool,
    processed_dir: str,
    process_txt_file_fn: FunctionType | None = None,
    process_pdf_file_fn: FunctionType = process_pdf_file_llmwhisperer,
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
                        file_csv_output = process_txt_file_llmwhisperer(
                            file_txt_output
                        )
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
                upload_csv_to_bigquery(file_csv_output)
                print("Uploaded to BigQuery.")
                shutil.move(
                    file_csv_output,
                    file_csv_processed_output,
                )
