from pathlib import Path
import json
from typing import Dict, List
from models.input_definition import InputDefinition
from models.output_definition import OutputDefinition

class FileManager:
    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
        self.manifest_path = self.config_dir / "manifest.json"

    def load_all(self) -> tuple[Dict[str, InputDefinition], List[OutputDefinition]]:
        if not self.manifest_path.exists():
            raise FileNotFoundError(f"Manifest file not found: {self.manifest_path}")

        data = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        if "inputs" not in data or "outputs" not in data:
            raise ValueError("Manifest must contain 'inputs' and 'outputs' arrays.")

        inputs: Dict[str, InputDefinition] = {}
        for item in data["inputs"]:
            if not item.get("active", True):
                continue
            p = self.config_dir / item["description_file"]
            idef = InputDefinition.from_json_file(p)
            inputs[idef.id] = idef

        outputs: List[OutputDefinition] = []
        for item in data["outputs"]:
            if not item.get("active", True):
                continue
            p = self.config_dir / item["description_file"]
            odef = OutputDefinition.from_json_file(p)
            if odef.input_id not in inputs:
                raise ValueError(f"Output '{odef.id}' references unknown input_id '{odef.input_id}'.")
            outputs.append(odef)

        return inputs, outputs
