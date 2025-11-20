import os
import tempfile

import pandas as pd
from typer.testing import CliRunner

from main import app


def test_merger_run_command():
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create sample CSV files
        df1 = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        df2 = pd.DataFrame({"A": [5, 6], "B": [7, 8]})
        csv1 = os.path.join(temp_dir, "file1.csv")
        csv2 = os.path.join(temp_dir, "file2.csv")
        df1.to_csv(csv1, index=False)
        df2.to_csv(csv2, index=False)
        output_csv = os.path.join(temp_dir, "merged.csv")

        # Run the CLI merger command
        result = runner.invoke(
            app,
            ["merger", "run", "--path-dir", temp_dir, "--output", output_csv],
        )
        assert result.exit_code == 0
        assert os.path.exists(output_csv)

        # Validate merged CSV content
        merged_df = (
            pd.read_csv(output_csv)
            .sort_values(by=["A", "B"])
            .reset_index(drop=True)
        )
        expected_df = (
            pd.concat([df1, df2], ignore_index=True)
            .sort_values(by=["A", "B"])
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(merged_df, expected_df)
