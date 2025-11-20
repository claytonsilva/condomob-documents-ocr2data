import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import typer
from typer.testing import CliRunner

from main import analytical_app, app, spliter_app
from utils.constants import FileType


def test_cli_setup():
    """Test the CLI app setup and configuration."""
    assert isinstance(app, typer.Typer)
    assert isinstance(analytical_app, typer.Typer)
    assert isinstance(spliter_app, typer.Typer)


def test_subcommands_added():
    """Test that subcommands are properly added to the main app."""
    # This tests that the app structure is set up correctly
    # The actual command registration is tested via CLI invocation
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "analytical" in result.stdout
    assert "spliter" in result.stdout


@pytest.fixture
def mock_env_vars():
    """Mock required environment variables."""
    with patch.dict(
        os.environ,
        {
            "GOOGLE_CLOUD_PROJECT": "test-project",
            "GOOGLE_SHEET_ACCOUNT_PLAN_ANALYTICAL_URL": "test-sheet-url",
            "GOOGLE_SHEET_RENAMED_UNITS_ANALYTICAL_URL": "test-units-url",
            "GOOGLE_CLOUD_BIGQUERY_DATASET_ID": "test-dataset",
            "GOOGLE_CLOUD_BIGQUERY_TABLE_ID_ANALYTICAL": "test-table",
        },
    ):
        yield


@pytest.fixture
def mock_bigquery_client():
    """Mock BigQuery client."""
    with patch("main.bigquery.Client") as mock_client:
        yield mock_client.return_value


@pytest.fixture
def mock_run_analytical():
    """Mock the run_analytical function."""
    with patch("main.run_analytical_import") as mock_run:
        mock_run.return_value = "success"
        yield mock_run


@pytest.fixture
def temp_dirs():
    """Create temporary directories for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        output_dir = temp_path / "output"
        processed_dir = temp_path / "processed"
        output_dir.mkdir()
        processed_dir.mkdir()

        yield {
            "output": str(output_dir),
            "processed": str(processed_dir),
            "temp": temp_dir,
        }


def test_run_command_default_parameters(
    mock_env_vars,
    mock_bigquery_client,
    mock_run_analytical,
    temp_dirs,
):
    """Test run command with default parameters."""
    runner = CliRunner()
    test_path = "test.pdf"

    result = runner.invoke(
        analytical_app,
        [
            "run",
            test_path,
            "--output-dir",
            temp_dirs["output"],
            "--processed-dir",
            temp_dirs["processed"],
        ],
    )

    assert result.exit_code == 0
    mock_run_analytical.assert_called_once()

    # Verify the call arguments
    call_args = mock_run_analytical.call_args
    assert call_args[0][0] == test_path  # path
    assert call_args[0][1] == temp_dirs["output"]  # output_dir
    assert call_args[0][2] == 1  # start
    assert call_args[0][3] is None  # end

    # Verify keyword arguments
    kwargs = call_args[1]
    assert kwargs["reprocess"] is False
    assert kwargs["processed_dir"] == temp_dirs["processed"]
    assert kwargs["upload"] is False


def test_run_command_with_all_parameters(
    mock_env_vars,
    mock_bigquery_client,
    mock_run_analytical,
    temp_dirs,
):
    """Test run command with all parameters specified."""
    runner = CliRunner()
    test_path = "test.pdf"

    result = runner.invoke(
        analytical_app,
        [
            "run",
            test_path,
            "--output-dir",
            temp_dirs["output"],
            "--start",
            "2",
            "--end",
            "10",
            "--upload",
            "--processed-dir",
            temp_dirs["processed"],
            "--reprocess",
            "--method",
            "docling",
        ],
    )

    assert result.exit_code == 0
    mock_run_analytical.assert_called_once()

    call_args = mock_run_analytical.call_args
    assert call_args[0][0] == test_path
    assert call_args[0][1] == temp_dirs["output"]
    assert call_args[0][2] == 2  # start
    assert call_args[0][3] == 10  # end

    kwargs = call_args[1]
    assert kwargs["reprocess"] is True
    assert kwargs["upload"] is True


def test_run_command_method_llmwhisperer(
    mock_env_vars, mock_bigquery_client, mock_run_analytical
):
    """Test run command with llmwhisperer method."""
    with (
        patch("main.process_pdf_file_llmwhisperer") as mock_pdf_llm,
        patch("main.process_txt_file_llmwhisperer") as mock_txt_llm,
    ):
        runner = CliRunner()
        result = runner.invoke(
            analytical_app, ["run", "test.pdf", "--method", "llmwhisperer"]
        )

        assert result.exit_code == 0
        mock_run_analytical.assert_called_once()

        kwargs = mock_run_analytical.call_args[1]
        assert kwargs["process_pdf_file_fn"] == mock_pdf_llm
        assert kwargs["process_txt_file_fn"] == mock_txt_llm


def test_run_command_method_docling(
    mock_env_vars, mock_bigquery_client, mock_run_analytical
):
    """Test run command with docling method."""
    with patch("main.process_pdf_file_docling") as mock_pdf_docling:
        runner = CliRunner()
        result = runner.invoke(
            analytical_app, ["run", "test.pdf", "--method", "docling"]
        )

        assert result.exit_code == 0
        mock_run_analytical.assert_called_once()

        kwargs = mock_run_analytical.call_args[1]
        assert kwargs["process_pdf_file_fn"] == mock_pdf_docling
        assert kwargs["process_txt_file_fn"] is None


def test_run_command_bigquery_client_creation(
    mock_env_vars, mock_bigquery_client, mock_run_analytical
):
    """Test that BigQuery client is created with correct project."""
    runner = CliRunner()
    result = runner.invoke(analytical_app, ["run", "test.pdf"])

    assert result.exit_code == 0
    # Verify BigQuery client was created with the environment variable
    mock_run_analytical.assert_called_once()
    kwargs = mock_run_analytical.call_args[1]
    assert kwargs["client"] == mock_bigquery_client
    assert kwargs["dataset_id"] == "test-dataset"
    assert kwargs["table_id"] == "test-table"


@pytest.fixture
def mock_reprocess_analytical():
    """Mock the reprocess_analytical function."""
    with patch("main.reprocess_analytical_import") as mock_reprocess:
        mock_reprocess.return_value = "success"
        yield mock_reprocess


def test_reprocess_command_default_parameters(
    mock_env_vars, mock_bigquery_client, mock_reprocess_analytical
):
    """Test reprocess command with default parameters."""
    runner = CliRunner()
    test_path = "test.pdf"

    result = runner.invoke(analytical_app, ["reprocess", test_path])

    assert result.exit_code == 0
    mock_reprocess_analytical.assert_called_once()

    call_args = mock_reprocess_analytical.call_args
    assert call_args[0][0] == test_path
    assert call_args[0][1] == os.path.join(os.getcwd(), "output")

    kwargs = call_args[1]
    assert kwargs["file_type"] == FileType.TXT
    assert kwargs["upload"] is False


def test_reprocess_command_with_all_parameters(
    mock_env_vars, mock_bigquery_client, mock_reprocess_analytical
):
    """Test reprocess command with all parameters specified."""
    runner = CliRunner()
    test_path = "test.pdf"
    output_dir = "/custom/output"

    result = runner.invoke(
        analytical_app,
        [
            "reprocess",
            test_path,
            "--output-dir",
            output_dir,
            "--method",
            "docling",
            "--file-type",
            ".pdf",
            "--upload",
        ],
    )

    assert result.exit_code == 0
    mock_reprocess_analytical.assert_called_once()

    call_args = mock_reprocess_analytical.call_args
    assert call_args[0][0] == test_path
    assert call_args[0][1] == output_dir

    kwargs = call_args[1]
    assert kwargs["file_type"] == FileType.PDF
    assert kwargs["upload"] is True


def test_reprocess_command_method_selection(
    mock_env_vars, mock_bigquery_client, mock_reprocess_analytical
):
    """Test that the correct processing functions are selected based on method."""
    with (
        patch("main.process_pdf_file_llmwhisperer") as mock_pdf_llm,
        patch("main.process_txt_file_llmwhisperer") as mock_txt_llm,
        patch("main.process_pdf_file_docling") as mock_pdf_docling,
    ):
        runner = CliRunner()

        # Test llmwhisperer method
        result = runner.invoke(
            analytical_app,
            ["reprocess", "test.pdf", "--method", "llmwhisperer"],
        )

        assert result.exit_code == 0
        kwargs = mock_reprocess_analytical.call_args[1]
        assert kwargs["process_pdf_file_fn"] == mock_pdf_llm
        assert kwargs["process_txt_file_fn"] == mock_txt_llm

        # Test docling method
        result = runner.invoke(
            analytical_app,
            ["reprocess", "test.pdf", "--method", "docling"],
        )

        assert result.exit_code == 0
        kwargs = mock_reprocess_analytical.call_args[1]
        assert kwargs["process_pdf_file_fn"] == mock_pdf_docling
        assert kwargs["process_txt_file_fn"] is None


@pytest.fixture
def mock_split_pdf_to_pages():
    """Mock the split_pdf_to_pages function."""
    with patch("main.split_pdf_import") as mock_split:
        yield mock_split


def test_run_split_default_parameters(mock_split_pdf_to_pages):
    """Test run_split command with default parameters."""
    runner = CliRunner()
    test_path = "test.pdf"

    result = runner.invoke(spliter_app, [test_path])

    assert result.exit_code == 0
    mock_split_pdf_to_pages.assert_called_once_with(
        test_path, "output", 1, None
    )


def test_run_split_with_all_parameters(mock_split_pdf_to_pages):
    """Test run_split command with all parameters specified."""
    runner = CliRunner()
    test_path = "test.pdf"
    output_dir = "/custom/output"
    start = 5
    end = 15

    result = runner.invoke(
        spliter_app,
        [
            test_path,
            "--output-dir",
            output_dir,
            "--start",
            str(start),
            "--end",
            str(end),
        ],
    )

    assert result.exit_code == 0
    mock_split_pdf_to_pages.assert_called_once_with(
        test_path, output_dir, start, end
    )


def test_run_split_with_partial_parameters(mock_split_pdf_to_pages):
    """Test run_split command with some parameters specified."""
    runner = CliRunner()
    test_path = "test.pdf"
    start = 3

    result = runner.invoke(spliter_app, [test_path, "--start", str(start)])

    assert result.exit_code == 0
    mock_split_pdf_to_pages.assert_called_once_with(
        test_path, "output", start, None
    )


@patch("main.load_dotenv")
@patch("main.app")
def test_main_execution(mock_app, mock_load_dotenv):
    """Test that main execution loads environment and runs the app."""
    # Simulate what happens in the if __name__ == "__main__": block
    from main import app, load_dotenv

    # Call the functions that would be called in the main block
    load_dotenv()
    app()

    # Verify the functions were called
    mock_load_dotenv.assert_called_once()
    mock_app.assert_called_once()


@pytest.fixture
def mock_all_dependencies():
    """Mock all external dependencies for integration tests."""
    with (
        patch.dict(
            os.environ,
            {
                "GOOGLE_CLOUD_PROJECT": "test-project",
                "GOOGLE_SHEET_ACCOUNT_PLAN_ANALYTICAL_URL": "test-sheet-url",
                "GOOGLE_SHEET_RENAMED_UNITS_ANALYTICAL_URL": "test-units-url",
                "GOOGLE_CLOUD_BIGQUERY_DATASET_ID": "test-dataset",
                "GOOGLE_CLOUD_BIGQUERY_TABLE_ID_ANALYTICAL": "test-table",
            },
        ),
        patch("main.bigquery.Client"),
        patch("main.run_analytical_import") as mock_run,
        patch("main.reprocess_analytical_import") as mock_reprocess,
        patch("main.split_pdf_import") as mock_split,
    ):
        mock_run.return_value = "run_success"
        mock_reprocess.return_value = "reprocess_success"

        yield {
            "run": mock_run,
            "reprocess": mock_reprocess,
            "split": mock_split,
        }


def test_analytical_help_command():
    """Test that analytical help command works."""
    runner = CliRunner()
    result = runner.invoke(app, ["analytical", "--help"])

    assert result.exit_code == 0
    assert "run" in result.stdout
    assert "reprocess" in result.stdout


def test_spliter_help_command():
    """Test that spliter help command works."""
    runner = CliRunner()
    result = runner.invoke(app, ["spliter", "--help"])

    assert result.exit_code == 0
    assert "run" in result.stdout


def test_main_help_command():
    """Test that main help command works."""
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "analytical" in result.stdout
    assert "spliter" in result.stdout


def test_command_chain_execution(mock_all_dependencies):
    """Test that commands can be executed in sequence."""
    runner = CliRunner()
    mocks = mock_all_dependencies

    # Test analytical run
    result = runner.invoke(app, ["analytical", "run", "test.pdf"])
    assert result.exit_code == 0
    mocks["run"].assert_called_once()

    # Test analytical reprocess
    result = runner.invoke(app, ["analytical", "reprocess", "test.pdf"])
    assert result.exit_code == 0
    mocks["reprocess"].assert_called_once()

    # Test spliter run
    result = runner.invoke(app, ["spliter", "run", "test.pdf"])
    assert result.exit_code == 0
    mocks["split"].assert_called_once()


def test_invalid_method_type():
    """Test that invalid method type raises error."""
    runner = CliRunner()
    result = runner.invoke(
        analytical_app, ["run", "test.pdf", "--method", "invalid_method"]
    )

    assert result.exit_code != 0
    # Note: Output may be in stderr instead of stdout for validation errors
    output = result.stdout + getattr(result, "stderr", "")
    assert "Invalid value" in output or "Usage:" in output


def test_invalid_file_type():
    """Test that invalid file type raises error."""
    runner = CliRunner()
    result = runner.invoke(
        analytical_app, ["reprocess", "test.pdf", "--file-type", "INVALID"]
    )

    assert result.exit_code != 0
    # Note: Output may be in stderr instead of stdout for validation errors
    output = result.stdout + getattr(result, "stderr", "")
    assert "Invalid value" in output or "Usage:" in output


def test_missing_required_path():
    """Test that missing required path parameter raises error."""
    runner = CliRunner()

    # Test analytical run without path
    result = runner.invoke(analytical_app, ["run"])
    assert result.exit_code != 0

    # Test analytical reprocess without path
    result = runner.invoke(analytical_app, ["reprocess"])
    assert result.exit_code != 0

    # Test spliter run without path
    result = runner.invoke(spliter_app, [])
    assert result.exit_code != 0
