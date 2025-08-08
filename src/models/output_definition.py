from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from pathlib import Path
import json

@dataclass(frozen=True)
class OutputColumn:
    name: str
    source: Optional[str] = None
    compute: Optional[str] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "OutputColumn":
        if "name" not in d:
            raise ValueError("Output column requires `name`")
        if ("source" in d) and ("compute" in d):
            raise ValueError("Output column must have either `source` or `compute`, not both.")
        if ("source" not in d) and ("compute" not in d):
            raise ValueError("Output column needs one of `source` or `compute`.")
        return cls(name=d["name"], source=d.get("source"), compute=d.get("compute"))

@dataclass(frozen=True)
class OutputDefinition:
    id: str
    input_id: str
    output_file_name: str
    processor_module: str
    columns: List[OutputColumn]
    omit_unmapped: bool = True
    delimiter: str = ","

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "OutputDefinition":
        req = {"id","input_id","output_file_name","processor_module","columns"}
        miss = req - d.keys()
        if miss:
            raise ValueError(f"Missing keys in output definition: {sorted(miss)}")
        cols = [OutputColumn.from_dict(c) for c in d["columns"]]
        return cls(
            id=d["id"].strip(),
            input_id=d["input_id"].strip(),
            output_file_name=d["output_file_name"],
            processor_module=d["processor_module"],
            columns=cols,
            delimiter=d.get("delimiter", ",") ,
            omit_unmapped=bool(d.get("omit_unmapped", True))
        )

    @classmethod
    def from_json_file(cls, path: str | Path) -> "OutputDefinition":
        path = Path(path)
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls.from_dict(data)
