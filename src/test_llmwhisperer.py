from unittest.mock import MagicMock, patch

from unstract.llmwhisperer.client_v2 import LLMWhispererClientException

from llmwhisperer import process_file


@patch("llmwhisperer.LLMWhispererClientV2")
@patch("llmwhisperer.load_dotenv")
def test_process_file_success(mock_load_dotenv, mock_client_cls, tmp_path):
    # Arrange
    mock_client = MagicMock()
    mock_client.whisper.return_value = {
        "extraction": {"result_text": "extracted text"}
    }
    mock_client_cls.return_value = mock_client
    test_pdf = tmp_path / "test.pdf"
    test_pdf.write_text("dummy pdf content")

    # Act
    output_path = process_file(str(test_pdf))

    # Assert
    assert output_path == str(test_pdf).replace(".pdf", ".txt")
    with open(output_path) as f:
        assert f.read() == "extracted text"


@patch("llmwhisperer.LLMWhispererClientV2")
def test_process_file_exception(mock_client_cls, tmp_path):
    # Arrange
    mock_client = MagicMock()
    mock_client.whisper.side_effect = LLMWhispererClientException("API error")
    mock_client_cls.return_value = mock_client
    test_pdf = tmp_path / "test.pdf"
    test_pdf.write_text("dummy pdf content")

    output_path = process_file(str(test_pdf))
    # Assert
    assert output_path == ""

