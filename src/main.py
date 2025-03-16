import camelot
from enum import Enum
import re
from pandas import Series
from constants import ExtracTypeRow, COLLUMNS
from extract import get_current_title, has_truncated_rows, identify_row, merge_truncated


def extract_tables_from_pdf(pdf_path: str, pages: str, flavor='stream', table_areas: list = None):
    """
    Extracts tables from a PDF file using the camelot library.

    Args:
    pdf_path (str): The path to the PDF file.
    pages (str): The pages to extract tables from. Can be '1', 'all', '1,2,3', etc.
    flavor (str): The flavor of the PDF file. Default is 'stream'.
    table_areas : list, optional (default: None)
        List of table area strings of the form x1,y1,x2,y2
        where (x1, y1) -> left-top and (x2, y2) -> right-bottom
        in PDF coordinate space.

    Returns:
    list: A list of DataFrames, where each DataFrame represents a table extracted from the PDF.
    """
    try:
        tables = camelot.read_pdf(
            pdf_path, pages=pages, flavor=flavor, table_areas=table_areas)
        return [table.df for table in tables]
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


def __main():
    # Especifique o caminho para o seu arquivo PDF e a(s) página(s) a serem lidas.
    # O parâmetro pages pode ser '1', 'all', '1,2,3', etc.
    arquivo_pdf = "./files/2024-01-analitico-v8.pdf"
    table_areas = ['0,780,600,30']
    pages = '39'
    flavor = 'stream'
    max_merge = 3

    # Extrai as tabelas do PDF
    tables = extract_tables_from_pdf(
        arquivo_pdf, pages=pages, flavor=flavor, table_areas=table_areas)

    # Iremos fazer em cada tabela duas percorridas de loop
    for i, table in enumerate(tables, start=1):
        table_title_number = ''
        table_title_description = ''
        table_output = table.copy()
        table_output.columns = COLLUMNS
        # inserindo previamente a colunas já com os tipos definidos
        table_output.insert(
            0, 'tipoDado', table_output.apply(identify_row, axis=1))
        table_output.insert(
            0, 'ContaContabilCompleto', table_output.apply(lambda row: get_current_title(table_output, row), axis=1))
        # segundo loop é somente para corrigir textos truncados
        output_index = has_truncated_rows(table_output, 0)
        while output_index > 0:
            table_output = merge_truncated(
                table_output, output_index, max_merge)
            output_index = has_truncated_rows(
                table_output,
                output_index + 1)
        # remover dados que não serão mais usados
        table_output.drop(
            table_output[table_output['tipoDado'] != ExtracTypeRow.ROW].index, inplace=True)
        # no final insere as colunas sumárias da tabela
        table_output.insert(0, 'ContaContabilDescritivo',
                            table_output.apply(lambda row: re.match(r"^(\d+\.\d[0-9.]*)( - )(.*$)", row['ContaContabilCompleto']).group(1), axis=1))
        table_output.insert(0, 'ContaContabil',
                            table_output.apply(lambda row: re.match(r"^(\d+\.\d[0-9.]*)( - )(.*$)", row['ContaContabilCompleto']).group(3), axis=1))
        table_output.drop(
            columns=['tipoDado', 'ContaContabilCompleto'], inplace=True)

        print(table_output)


if __name__ == "__main__":
    __main()
