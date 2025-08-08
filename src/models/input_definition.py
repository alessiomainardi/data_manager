from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Any
from pathlib import Path
import json

from .column_definition import ColumnDefinition

@dataclass(frozen=True)
class InputDefinition:
    id: str
    file_name: str
    delimiter: str
    encoding: str
    has_headers: bool
    decimal_separator: str
    thousands_separator: str
    date_format: str
    columns: List[ColumnDefinition] = field(default_factory=list)

    @staticmethod
    def _validate_payload(p: Dict[str, Any]) -> None:
        req = {"id","file_name","delimiter","encoding","has_headers",
               "decimal_separator","thousands_separator","date_format","columns"}
        miss = req - p.keys()
        if miss:
            raise ValueError(f"Missing keys in input definition: {sorted(miss)}")

    @staticmethod
    def _validate_columns(columns: List[ColumnDefinition]) -> None:
        pos = [c.position for c in columns]
        if sorted(pos) != list(range(1, len(pos)+1)):
            raise ValueError("Column positions must be contiguous starting at 1.")
        names = [c.name.lower() for c in columns]
        if len(set(names)) != len(names):
            raise ValueError("Duplicate column names (case-insensitive).")

    @classmethod
    def from_dict(cls, p: Dict[str, Any]) -> "InputDefinition":
        cls._validate_payload(p)
        cols = [ColumnDefinition.from_dict(c) for c in p["columns"]]
        cls._validate_columns(cols)
        return cls(
            id=p["id"].strip(),
            file_name=p["file_name"].strip(),
            delimiter=p["delimiter"],
            encoding=p["encoding"].strip(),
            has_headers=bool(p["has_headers"]),
            decimal_separator=p["decimal_separator"],
            thousands_separator=p["thousands_separator"],
            date_format=p["date_format"],
            columns=cols
        )

    @classmethod
    def from_json_file(cls, path: str | Path) -> "InputDefinition":
        path = Path(path)
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls.from_dict(data)
