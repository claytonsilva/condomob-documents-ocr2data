import pandas as pd
import pytest

from rp_transformers import analytical


@pytest.fixture
def sample_csv(tmp_path):
    data = [
        {
            "Participante": "Un. 888-QD88-LT88",
            "ContaContabil": "100",
            "file": "page_99_2023-01.csv",
        },
        {
            "Participante": "Un. 999-QD99-LT99",
            "ContaContabil": "200",
            "file": "page_99_2023-01.csv",
        },
    ]
    df = pd.DataFrame(data)
    csv_path = tmp_path / "page_99_2023-01"
    df.to_csv(csv_path, index=False)
    return str(csv_path)


@pytest.fixture
def accounts_configuration_csv(tmp_path):
    data = [
        {
            "Natureza": "R",
            "CompoeTaxa": "True",
            "AcordadoAssembleia": "True",
            "NaturezaDescritivo": "Receita",
            "ContaContabil": "100",
            "ContaContabilDescritivo": "100",
            "ContaContabilGrupo": "G1",
            "ContaContabilGrupoDescritivo": "G1",
            "ContaContabilNormalizado": "100",
        },
        {
            "Natureza": "D",
            "CompoeTaxa": "True",
            "AcordadoAssembleia": "True",
            "NaturezaDescritivo": "Despesa",
            "ContaContabil": "200",
            "ContaContabilDescritivo": "200",
            "ContaContabilGrupo": "G2",
            "ContaContabilGrupoDescritivo": "G2",
            "ContaContabilNormalizado": "200",
        },
    ]
    df = pd.DataFrame(data)
    csv_path = tmp_path / "accounts.csv"
    df.to_csv(csv_path, index=False)
    return str(csv_path)


@pytest.fixture
def units_renamed_csv(tmp_path):
    data = [
        {"Participante": "Un. R22-888-QD88-LT88"},
        {"Participante": "Un. R16-999-QD99-LT99"},
    ]
    df = pd.DataFrame(data)
    csv_path = tmp_path / "units.csv"
    df.to_csv(csv_path, index=False)
    return str(csv_path)


def test_transform_with_all_configs(
    sample_csv, accounts_configuration_csv, units_renamed_csv
):
    # Patch pandas in the function's namespace
    # Call the function
    analytical.transform_generated_analytical_data(
        sample_csv,
        accounts_configuration_csv,
        units_renamed_csv,
    )
    # Read the output
    df = pd.read_csv(sample_csv, dtype=str).fillna("")
    # Check that columns were renamed and added
    assert "Participante" in df.columns
    assert df["Participante"].iloc[0] == "Un. R22-888-QD88-LT88"
    assert "ContaContabil" in df.columns
    assert "PeriodoPrestacaoContas" in df.columns
    assert df["PeriodoPrestacaoContas"].iloc[0] == "2023-01"


def test_transform_with_no_configs(sample_csv):
    # Should not raise or change much
    analytical.transform_generated_analytical_data(
        sample_csv,
        "",
        "",
    )
    df = pd.read_csv(sample_csv, dtype=str).fillna("")
    assert "Participante" in df.columns
    assert "ContaContabil" in df.columns


def test_transform_with_only_units(sample_csv, units_renamed_csv):
    df_init = pd.read_csv(sample_csv, dtype=str).fillna("")
    analytical.transform_generated_analytical_data(
        sample_csv,
        "",
        units_renamed_csv,
    )
    df = pd.read_csv(sample_csv, dtype=str).fillna("")
    assert df_init["Participante"].iloc[0] == "Un. 888-QD88-LT88"
    assert df["Participante"].iloc[0] == "Un. R22-888-QD88-LT88"
    assert "ContaContabil" in df.columns


def test_transform_with_only_accounts(sample_csv, accounts_configuration_csv):
    analytical.transform_generated_analytical_data(
        sample_csv,
        accounts_configuration_csv,
        "",
    )
    df = pd.read_csv(sample_csv, dtype=str).fillna("")
    assert "PeriodoPrestacaoContas" in df.columns
    assert df["PeriodoPrestacaoContas"].iloc[0] == "2023-01"
