from importlib import import_module
import pandas as pd
from models.output_definition import OutputDefinition

class DatasetBuilder:
    def __init__(self, output_def: OutputDefinition):
        self.output_def = output_def
        self.proc = import_module(output_def.processor_module)
        self.row_filter = getattr(self.proc, "should_drop_row", None)

        # pré-checar funções compute
        for oc in self.output_def.columns:
            if oc.compute and not hasattr(self.proc, oc.compute):
                raise AttributeError(
                    f"Compute function '{oc.compute}' not found in module '{self.output_def.processor_module}'"
                )

    def build(self, df_in: pd.DataFrame) -> pd.DataFrame:
        df = df_in.copy()
        # 1) filtrar linhas (se houver)
        if self.row_filter:
            mask_drop = df.apply(lambda r: bool(self.row_filter(r)), axis=1)
            df = df[~mask_drop].reset_index(drop=True)

        # 2) montar colunas de saída
        out = {}
        for oc in self.output_def.columns:
            if oc.source:
                out[oc.name] = df[oc.source]
            elif oc.compute:
                fn = getattr(self.proc, oc.compute)
                out[oc.name] = df.apply(lambda r: fn(r), axis=1)
            else:
                raise ValueError("Output column must define `source` or `compute`.")

        return pd.DataFrame(out)
