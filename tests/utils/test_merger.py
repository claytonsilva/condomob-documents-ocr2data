import os
import tempfile

import pandas as pd

from utils.merger import merge_document


# the order of the elements is not important, so we sort before test
def test_merge_document():
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create sample CSV files
        df1 = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        df2 = pd.DataFrame({"A": [5, 6], "B": [7, 8]})
        csv1 = os.path.join(temp_dir, "file1.csv")
        csv2 = os.path.join(temp_dir, "file2.csv")
        df1.to_csv(csv1, index=False)
        df2.to_csv(csv2, index=False)
        output_csv = os.path.join(temp_dir, "merged.csv")

        # Call the function to test
        merge_document(temp_dir, output_csv)

        # Read the merged CSV
        merged_df = (
            pd.read_csv(output_csv).sort_values(by=["A", "B"]).reset_index()
        )
        # Concatenate manually for expected result
        expected_df = (
            pd.concat([df1, df2], ignore_index=True)
            .sort_values(by=["A", "B"])
            .reset_index()
        )
        pd.testing.assert_series_equal(
            merged_df["A"],
            expected_df["A"],
        )
        pd.testing.assert_series_equal(
            merged_df["B"],
            expected_df["B"],
        )
