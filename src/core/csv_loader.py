import pandas as pd
from pathlib import Path
from models.input_definition import InputDefinition

class CSVLoader:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir

    def load_csv(self, definition: InputDefinition) -> pd.DataFrame:
        csv_path = self.data_dir / definition.file_name
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        df = pd.read_csv(
            csv_path,
            delimiter=definition.delimiter,
            encoding=definition.encoding,
            header=0 if definition.has_headers else None,
            decimal=definition.decimal_separator,
            thousands=definition.thousands_separator or None
        )

        if not definition.has_headers:
            df.columns = [c.name for c in definition.columns]

        if df.shape[1] != len(definition.columns):
            raise ValueError(f"CSV column count mismatch: expected {len(definition.columns)}, got {df.shape[1]}")

        if definition.has_headers:
            expected = [c.name for c in definition.columns]
            found = list(df.columns)
            if expected != found:
                raise ValueError(f"Header names mismatch.\nExpected: {expected}\nFound: {found}")

        self._validate_columns(df, definition)
        return df

    def _validate_columns(self, df: pd.DataFrame, definition: InputDefinition) -> None:
        for col_def in definition.columns:
            s = df.iloc[:, col_def.position - 1]

            if not col_def.nullable and s.isna().any():
                raise ValueError(f"Column '{col_def.name}' contains nulls but is not nullable.")

            if not col_def.allow_duplicates:
                dup = s[s.duplicated(keep=False)]
                if not dup.empty:
                    raise ValueError(f"Column '{col_def.name}' contains duplicates and does not allow them.")

            if col_def.type == "integer":
                try:
                    _ = s.dropna().astype("Int64")
                except Exception:
                    raise ValueError(f"Column '{col_def.name}' must contain only integers.")
            elif col_def.type == "numeric":
                try:
                    _ = pd.to_numeric(s.dropna(), errors="raise")
                except Exception:
                    raise ValueError(f"Column '{col_def.name}' must be numeric.")
            elif col_def.type == "date":
                try:
                    _ = pd.to_datetime(s.dropna(), format=definition.date_format, errors="raise")
                except Exception:
                    raise ValueError(f"Column '{col_def.name}' must follow date format {definition.date_format}.")
            elif col_def.type == "alphabetic":
                # regra simples: letras e espaços
                 nonnull = s.dropna().astype(str)
                # se quiser estrito com acentos, podemos usar `regex` lib
                # import re
                #if not nonnull.apply(lambda x: re.fullmatch(r"[A-Za-zÀ-ÖØ-öø-ÿ ]+", x) is not None).all():
                #    raise ValueError(f"Column '{col_def.name}' must be alphabetic (letters/spaces).")
            else:
                raise ValueError(f"Unknown column type '{col_def.type}' for column '{col_def.name}'.")
