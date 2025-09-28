from enum import Enum

from pandas import Series

FileType = Enum(
    "FILE_TYPE",
    [
        ("PDF", ".pdf"),
        ("TXT", ".txt"),
        ("CSV", ".csv"),
    ],
)

ExtracTypeRow = Enum(
    "EXTRACTED_TYPE_ROW",
    [
        ("TOTAL", 1),
        ("TITLE", 2),
        ("ROW", 3),
        ("HEADERS", 4),
        ("OTHERS", 7),
    ],
)
COLUMNS_ANALYTICAL = Series(
    ["Data", "Descrição", "Participante", "Documento", "Período", "Valor"]
)


class MethodType(str, Enum):
    llmwhisperer = "llmwhisperer"
    docling = "docling"
