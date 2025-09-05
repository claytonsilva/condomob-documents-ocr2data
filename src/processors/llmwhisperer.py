from unstract.llmwhisperer import LLMWhispererClientV2
from unstract.llmwhisperer.client_v2 import LLMWhispererClientException


def process_file(path: str) -> str:
    """
    Process a file using the LLMWhispererClientV2.
    Args:
        path (str): The path to the file to be processed.
    Returns:
        str: The response from the LLMWhispererClientV2.
    """
    # read more about llmwhisperer client configuration variables

    # os.environ["LLMWHISPERER_API_VERSION"] = dotenv_values().get("LLMWHISPERER_API_VERSION", "v2")
    # os.emviron["LLMWHISPERER_API_TIMEOUT"] = dotenv_values().get("LLMWHISPERER_API_TIMEOUT", "60")
    # os.environ["LLMWHISPERER_API_RETRIES"] = dotenv_values().get("LLMWHISPERER_API_RETRIES", "3")
    # os.environ["LLMWHISPERER_API_BACKOFF_FACTOR"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_FACTOR", "0.1")
    # os.environ["LLMWHISPERER_API_BACKOFF_MAX"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_MAX", "1.0")
    # os.environ["LLMWHISPERER_API_BACKOFF_JITTER"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_JITTER", "0.1")
    # os.environ["LLMWHISPERER_API_BACKOFF_MAX_RETRIES"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_MAX_RETRIES", "5")
    # os.environ["LLMWHISPERER_API_BACKOFF_STATUS_FORCELIST"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_STATUS_FORCELIST", "500,502,503,504")
    # os.environ["LLMWHISPERER_API_BACKOFF_METHOD"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_METHOD", "exponential")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_TIMEOUT"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_TIMEOUT", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_ERROR"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_ERROR", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_STATUS"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_STATUS", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_EXCEPTION"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_EXCEPTION", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_HTTP_ERROR"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_HTTP_ERROR", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_TIMEOUT_ERROR"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_TIMEOUT_ERROR", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_TIMEOUT"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_TIMEOUT", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_REFUSED"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_REFUSED", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_RESET"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_RESET", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_CLOSED"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_CLOSED", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_TIMEOUT_ERROR"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_TIMEOUT_ERROR", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_ERROR"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_ERROR", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_HTTP_ERROR"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_HTTP_ERROR", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_TIMEOUT"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_TIMEOUT", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_TIMEOUT"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_TIMEOUT", "True")
    # os.environ["LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_REFUSED"] = dotenv_values().get("LLMWHISPERER_API_BACKOFF_RETRY_ON_CONNECTION_REFUSED", "True")

    client = LLMWhispererClientV2()
    try:
        result = client.whisper(
            file_path=path,
            mode="table",
            lang="por",
            wait_for_completion=True,
            wait_timeout=200,
        )
        file_output = path.replace(".pdf", ".txt")
        with open(file_output, "w") as out_file:
            out_file.write(result["extraction"]["result_text"])
        return file_output
    except LLMWhispererClientException as e:
        print(e)
        return ""
