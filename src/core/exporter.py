from __future__ import annotations
from pathlib import Path
import pandas as pd
import math
from models.input_definition import InputDefinition
from models.output_definition import OutputDefinition

class Exporter:
    def __init__(self, delimiter: str = ",", encoding: str = "utf-8"):
        self.delimiter = delimiter
        self.encoding = encoding

    def _col_type_from_input(self, idef: InputDefinition, col_name: str) -> str | None:
        for c in idef.columns:
            if c.name == col_name:
                return c.type  # "alphabetic" | "integer" | "numeric" | "date"
        return None

    def _fmt_series(self, s: pd.Series, col_type: str | None, idef: InputDefinition) -> pd.Series:
        # Respect the source type (input JSON). If unknown, leave as-is.
        if col_type == "alphabetic":
            # Keep as readable text exactly as it came, no float/scientific.
            return s.astype("string[python]").fillna("")
        if col_type == "integer":
            # Format integers without .0 and no thousands; empty stays empty.
            def _fmt(v):
                if v is None or (isinstance(v, float) and math.isnan(v)): return ""
                # if it looks like a float but is integral, print as int
                try:
                    fv = float(str(v).replace(",", "."))
                    if fv.is_integer(): return str(int(fv))
                except Exception:
                    pass
                # if it’s already an int-ish string
                vs = str(v).strip()
                if vs.endswith(".0"): vs = vs[:-2]
                return vs
            return s.map(_fmt).astype("string[python]")
        if col_type == "numeric":
            # Normalize to the input’s decimal separator (no thousands)
            dec = idef.decimal_separator or "."
            thou = (idef.thousands_separator or "")
            def _fmt(v):
                if v is None or (isinstance(v, float) and math.isnan(v)): return ""
                vs = str(v).strip()
                # strip thousands, then unify decimal to dot to parse, then back to desired decimal
                if thou: vs = vs.replace(thou, "")
                # tolerate inputs already with dot/comma
                try:
                    x = float(vs.replace(",", "."))
                    out = f"{x:.2f}"
                    if dec != ".": out = out.replace(".", dec)
                    return out
                except Exception:
                    # fallback: pass through
                    return vs
            return s.map(_fmt).astype("string[python]")
        if col_type == "date":
            # Format using the input date_format
            fmt = idef.date_format or "%Y-%m-%d"
            def _fmt(v):
                if v is None or str(v).strip() == "": return ""
                ts = pd.to_datetime(v, errors="coerce")
                return "" if pd.isna(ts) else ts.strftime(fmt)
            return s.map(_fmt).astype("string[python]")
        # Unknown: leave as-is
        return s

    def export(self, df_out: pd.DataFrame, odef: OutputDefinition, idef: InputDefinition, out_path: Path):
        result = df_out.copy()

        # For each output column that comes from a source, respect the source type
        for oc in odef.columns:
            if getattr(oc, "source", None) and oc.name in result.columns:
                src_name = oc.source
                src_type = self._col_type_from_input(idef, src_name)
                result[oc.name] = self._fmt_series(result[oc.name], src_type, idef)

        # (optional) computed columns type hints
        export_types = getattr(odef, "export_types", None)  # e.g. {"PRECO":"numeric","MENSAL":"alphabetic"}
        if isinstance(export_types, dict):
            for col, t in export_types.items():
                if col in result.columns:
                    result[col] = self._fmt_series(result[col], t, idef)

        # write with the delimiter defined in the OutputDefinition (fallback to comma)
        sep = getattr(odef, "delimiter", ",")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(out_path, index=False, encoding=self.encoding, sep=sep)
