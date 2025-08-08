import pandas as pd
import regex as re
from pathlib import Path
from typing import Union

from models.file_definition import FileDefinition

class CSVLoader:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir

    def load_csv(self, definition: FileDefinition) -> pd.DataFrame:
        """
        Loads a CSV file according to its FileDefinition metadata,
        validates structure and returns a pandas DataFrame.
        """
        csv_path = self.data_dir / definition.file_name
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        df = pd.read_csv(
            csv_path,
            delimiter=definition.delimiter,
            encoding=definition.encoding,
            header=0 if definition.has_headers else None
        )

        # Assign header names if file has no headers
        if not definition.has_headers:
            df.columns = [col.name for col in definition.columns]

        # Validate number of columns
        if df.shape[1] != len(definition.columns):
            raise ValueError(
                f"CSV column count mismatch: expected {len(definition.columns)}, got {df.shape[1]}"
            )

        # Ensure column names match definition (if headers exist)
        if definition.has_headers:
            expected_names = [col.name for col in definition.columns]
            file_names = list(df.columns)
            if file_names != expected_names:
                raise ValueError(
                    f"Header names do not match definition.\nExpected: {expected_names}\nFound: {file_names}"
                )

        # Validate column types
        self._validate_types(df, definition)

        return df

    def _validate_types(self, df, definition: FileDefinition) -> None:
        """
        Validates the data types of each column according to the definition.
        """
        for col_def in definition.columns:
            col_name = col_def.name
            series = df.iloc[:, col_def.position - 1]

            if col_def.type == "integer":
                if not pd.api.types.is_integer_dtype(series.dropna().astype("Int64")):
                    raise ValueError(f"Column '{col_name}' must contain only integers.")

            elif col_def.type == "numeric":
                if not pd.api.types.is_numeric_dtype(series.dropna().astype(float)):
                    raise ValueError(f"Column '{col_name}' must be numeric.")

            elif col_def.type == "date":
                try:
                    pd.to_datetime(series.dropna(), errors="raise")
                except Exception:
                    raise ValueError(f"Column '{col_name}' must contain valid dates.")

            elif col_def.type == "alphabetic":
                READABLE_RE = re.compile(r"^[\p{L}\p{N}\p{M}\p{Po}\p{Pd}\p{Pc}\p{Sk}\p{Sm}\p{Sc}\s]+$")
                nonnull = series.dropna().astype(str)
                if not nonnull.map(lambda s: bool(READABLE_RE.fullmatch(s))).all():
                    raise ValueError(
                        f"Column '{col_name}' must contain readable text (letters, numbers, accents, spaces, symbols)."
                    )

            else:
                raise ValueError(f"Unknown column type '{col_def.type}' for column '{col_name}'.")
