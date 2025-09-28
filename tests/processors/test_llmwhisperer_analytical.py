# filepath: tests/processors/test_llmwhisperer_analytical.py
import os
from io import StringIO

import pandas as pd

from processors import llmwhisperer_analytical as lwa
from utils.constants import FileType

# In your test file (e.g., test_my_parser.py)
os.chdir(os.path.dirname(__file__))


def test_is_this_file_type_txt():
    ft: FileType = FileType.TXT

    assert lwa.is_this_file_type("file.txt", ft)
    assert not lwa.is_this_file_type("file.csv", ft)


def test_openFile(tmp_path):
    file_path = tmp_path / "test.txt"
    file_path.write_text("hello world", encoding="utf-8")
    sio = lwa.openFile(str(file_path))
    assert isinstance(sio, StringIO)
    assert sio.getvalue() == "hello world"


def test_split_blocks():
    with open("fixtures/sample-1.txt") as f:
        data = f.read()
        blocks = lwa.split_blocks(data)
        assert len(blocks) == 11
        assert "Rendimento CDB" in blocks[1]
        assert "Doação para Eventos" in blocks[3]


def test_clean_table_text_ascii():
    ascii_table = "| Col1 | Col2 |\n|------|------|\n|  1   |  2   |"
    cleaned = lwa.clean_table_text(ascii_table)
    assert "| Col1 | Col2 |" in cleaned


def test_concat_dataframe_cells():
    df = pd.DataFrame(
        {"Data": ["01/01/2020", None, None], "Col1": ["A", "B", "C"]}
    )
    result = lwa.concat_dataframe_cells(df.copy())
    assert isinstance(result, pd.DataFrame)


def test_extract_untitled_table():
    text = "| table |\n\n| another table |"
    result = lwa.extract_untitled_table(text)
    assert "| another table" not in result


def test_drop_invalid_rows():
    df = pd.DataFrame({"Data": ["01/01/2020", "invalid", None]})
    result = lwa.drop_invalid_rows(df)
    assert all(isinstance(x, str) for x in result["Data"])


def test_strip_string_cells():
    df = pd.DataFrame({"A": ["  a ", " b "]})
    result = lwa.strip_string_cells(df)
    assert all(x == x.strip() for x in result["A"])


def test_strip_columns_names():
    df = pd.DataFrame({" A ": [1], "B ": [2]})
    result = lwa.strip_columns_names(df)
    assert all(col == col.strip() for col in result.columns)


def test_find_similar_columns():
    lst = ["Col1", "Col1 .1", "Col2"]
    pattern = lwa.re.compile(r"( \.[0-9]+)$")
    result = lwa.find_similar_columns(lst, pattern)
    assert "Col1" in result


def test_convert_list_to_dict():
    with open("fixtures/sample-1.txt") as f:
        data = f.read()
        blocks = lwa.split_blocks(data)
        result = (
            lwa.convert_list_to_dict(
                [lwa.clean_table_text(block) for block in blocks]
            ),
        )
        assert isinstance(result, tuple)


def test_clean_total_ascii_table():
    table = "TOTAL\n| Col |\n|----|\n| 1  |"
    cleaned = lwa.clean_total_ascii_table(table)
    assert "TOTAL" not in cleaned


def test_data_processing():
    with open("fixtures/sample-1.txt") as f:
        data = f.read()
        blocks = lwa.split_blocks(data)

        result = lwa.data_processing(
            lwa.convert_list_to_dict(
                [lwa.clean_table_text(block) for block in blocks]
            ),
            "test.pdf",
        )
        assert isinstance(result, pd.DataFrame)
        assert result["file"][0] == "test.pdf"
        assert result["Participante"][0] == ""

    with open("fixtures/sample-1-extra-column.txt") as f:
        data = f.read()
        blocks = lwa.split_blocks(data)

        result = lwa.data_processing(
            lwa.convert_list_to_dict(
                [lwa.clean_table_text(block) for block in blocks]
            ),
            "test2.pdf",
        )
        assert isinstance(result, pd.DataFrame)
        assert result["file"][0] == "test2.pdf"
        assert result["Participante"][0] == "JOSÉ ORLANDO DA SILVA"
