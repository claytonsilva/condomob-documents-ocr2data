import os
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

import analytical
from utils.constants import FileType


@pytest.mark.parametrize(
    "filename,filetype,expected",
    [
        ("file.pdf", FileType.PDF, True),
        ("file.txt", FileType.TXT, True),
        ("file.csv", FileType.CSV, True),
        ("file.doc", FileType.PDF, False),
    ],
)
def test_is_this_file_type(filename, filetype, expected):
    assert analytical.is_this_file_type(filename, filetype) == expected


def test_reprocess_pdf(monkeypatch, tmp_path):
    # Setup
    source_dir = tmp_path / "source"
    output_dir = tmp_path / "output"
    source_dir.mkdir()
    output_dir.mkdir()
    pdf_file = source_dir / "test.pdf"
    pdf_file.write_text("dummy")
    process_pdf_file_fn = mock.Mock(return_value=str(source_dir / "test.txt"))
    shutil_move = mock.Mock()
    monkeypatch.setattr(analytical, "shutil", mock.Mock(move=shutil_move))
    monkeypatch.setattr(analytical, "is_this_file_type", lambda f, _: True)
    # Run
    analytical.reprocess(
        str(source_dir),
        str(output_dir),
        None,
        process_pdf_file_fn,
        FileType.PDF,
        "conf",
        "units",
        False,
        mock.Mock(),
        "dataset",
        "table",
    )
    process_pdf_file_fn.assert_called_once()
    shutil_move.assert_called()


def test_reprocess_txt(monkeypatch, tmp_path):
    source_dir = tmp_path / "source"
    output_dir = tmp_path / "output"
    source_dir.mkdir()
    output_dir.mkdir()
    txt_file = source_dir / "test.txt"
    txt_file.write_text("dummy")
    process_txt_file_fn = mock.Mock(return_value=str(source_dir / "test.csv"))
    shutil_move = mock.Mock()
    monkeypatch.setattr(analytical, "shutil", mock.Mock(move=shutil_move))
    monkeypatch.setattr(analytical, "is_this_file_type", lambda f, _: True)
    monkeypatch.setattr(
        analytical, "process_txt_file_llmwhisperer", process_txt_file_fn
    )
    analytical.reprocess(
        str(source_dir),
        str(output_dir),
        process_txt_file_fn,
        mock.Mock(),
        FileType.TXT,
        "conf",
        "units",
        False,
        mock.Mock(),
        "dataset",
        "table",
    )
    process_txt_file_fn.assert_called_once()
    shutil_move.assert_called()


def test_reprocess_csv(monkeypatch, tmp_path):
    source_dir = tmp_path / "source"
    output_dir = tmp_path / "output"
    source_dir.mkdir()
    output_dir.mkdir()
    csv_file = source_dir / "test.csv"
    csv_file.write_text("dummy")
    transform_fn = mock.Mock()
    clear_fn = mock.Mock()
    upload_fn = mock.Mock()
    shutil_move = mock.Mock()
    monkeypatch.setattr(analytical, "shutil", mock.Mock(move=shutil_move))
    monkeypatch.setattr(analytical, "is_this_file_type", lambda f, _: True)
    monkeypatch.setattr(
        analytical, "transform_generated_analytical_data", transform_fn
    )
    monkeypatch.setattr(
        analytical, "clear_data_analytical_from_file", clear_fn
    )
    monkeypatch.setattr(analytical, "upload_csv_to_bigquery", upload_fn)
    analytical.reprocess(
        str(source_dir),
        str(output_dir),
        None,
        mock.Mock(),
        FileType.CSV,
        "conf",
        "units",
        True,
        mock.Mock(),
        "dataset",
        "table",
    )
    transform_fn.assert_called_once()
    clear_fn.assert_called_once()
    upload_fn.assert_called_once()
    shutil_move.assert_called()


def make_dummy_pdf_pages(tmp_path, n):
    pdfs = []
    for i in range(n):
        pdf_path = tmp_path / f"page_{i + 1}.pdf"
        pdf_path.write_bytes(b"dummy pdf content")
        pdfs.append(str(pdf_path))
    return pdfs


@pytest.fixture
def tmp_dirs(tmp_path):
    processed_dir = tmp_path / "processed"
    output_dir = tmp_path / "output"
    processed_dir.mkdir()
    output_dir.mkdir()
    return str(output_dir), str(processed_dir)


@pytest.fixture
def dummy_bigquery_client():
    return MagicMock()


@pytest.fixture
def dummy_functions():
    def process_pdf_file_fn(_, txt_path):
        with open(txt_path, "w") as f:
            f.write("dummy text")
        return txt_path

    def process_txt_file_fn(txt_path):
        csv_path = txt_path.replace(".txt", ".csv")
        with open(csv_path, "w") as f:
            f.write("dummy csv")
        return csv_path

    return process_pdf_file_fn, process_txt_file_fn


@patch("shutil.move")
@patch("os.path.exists")
@patch("analytical.split_pdf_to_pages")
@patch("analytical.transform_generated_analytical_data")
@patch("analytical.upload_csv_to_bigquery")
def test_run_basic(
    mock_upload,
    mock_transform,
    mock_split,
    mock_exists,
    mock_move,
    tmp_dirs,
    dummy_bigquery_client,
    dummy_functions,
):
    output_dir, processed_dir = tmp_dirs
    process_pdf_file_fn, process_txt_file_fn = dummy_functions
    # Simulate 2 pdf pages
    mock_split.return_value = [
        os.path.join(output_dir, f"page_{i + 1}.pdf") for i in range(2)
    ]

    # Simulate file existence
    def exists_side_effect(path):
        return path is not None

    mock_exists.side_effect = exists_side_effect

    analytical.run(
        path="dummy.pdf",
        output_dir=output_dir,
        start=1,
        end=2,
        reprocess=False,
        processed_dir=processed_dir,
        process_txt_file_fn=process_txt_file_fn,
        process_pdf_file_fn=process_pdf_file_fn,
        upload=True,
        analytical_accounts_configuration="conf",
        analytical_units_renamed_list="units",
        client=dummy_bigquery_client,
        dataset_id="ds",
        table_id="tbl",
    )
    assert mock_split.called
    assert mock_transform.call_count == 2
    assert mock_upload.call_count == 2
    assert mock_move.called


@patch("shutil.move")
@patch("os.path.exists")
@patch("analytical.split_pdf_to_pages")
@patch("analytical.transform_generated_analytical_data")
@patch("analytical.upload_csv_to_bigquery")
def test_run_reprocess(
    mock_upload,
    mock_transform,
    mock_split,
    mock_exists,
    mock_move,
    tmp_dirs,
    dummy_bigquery_client,
    dummy_functions,
):
    output_dir, processed_dir = tmp_dirs
    process_pdf_file_fn, process_txt_file_fn = dummy_functions
    mock_split.return_value = [os.path.join(output_dir, "page_1.pdf")]

    def exists_side_effect(path):
        return "processed" in path

    mock_exists.side_effect = exists_side_effect

    analytical.run(
        path="dummy.pdf",
        output_dir=output_dir,
        start=1,
        end=1,
        reprocess=True,
        processed_dir=processed_dir,
        process_txt_file_fn=process_txt_file_fn,
        process_pdf_file_fn=process_pdf_file_fn,
        upload=False,
        analytical_accounts_configuration="conf",
        analytical_units_renamed_list="units",
        client=dummy_bigquery_client,
        dataset_id="ds",
        table_id="tbl",
    )
    assert mock_move.called
    assert mock_transform.called
    assert not mock_upload.called
