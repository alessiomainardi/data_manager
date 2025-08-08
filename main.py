# main.py
import sys
from pathlib import Path
from core.exporter import Exporter

# --- 1) Garanta que <raiz>/src esteja no sys.path ANTES dos imports do projeto ---
ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# --- 2) Imports do projeto ---
from core.file_manager import FileManager
from core.csv_loader import CSVLoader
from core.dataset_builder import DatasetBuilder

if __name__ == "__main__":
    # --- 3) Pastas do projeto ---
    config_dir = ROOT / "config"
    data_in = ROOT / "data" / "incoming"
    data_out = ROOT / "data" / "output"
    data_out.mkdir(parents=True, exist_ok=True)

    # --- 4) Carrega definições ---
    fm = FileManager(config_dir=config_dir)
    inputs_map, outputs = fm.load_all()  # dict[input_id] -> InputDefinition, list[OutputDefinition]

    # --- 5) Loader e cache de DataFrames por input_id ---
    loader = CSVLoader(data_dir=data_in)
    cache_df = {}

    exporter = Exporter()  # encoding default utf-8

    # --- 6) Processa cada OutputDefinition ---
    for odef in outputs:
        idef = inputs_map[odef.input_id]

        # cria o builder apenas com os parâmetros que ele realmente aceita
        builder = DatasetBuilder(idef, odef)
        df_out = builder.build()

        out_path = data_out / odef.output_file_name
        exporter.export(df_out, odef, idef, out_path)
        print(f"[OK] Saved -> {out_path}")
    print("\nDone.")
