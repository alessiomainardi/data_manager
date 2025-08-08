from __future__ import annotations
from dataclasses import dataclass
from typing import Literal, Dict, Any

ColumnType = Literal["alphabetic", "integer", "date", "numeric"]

@dataclass(frozen=True)
class ColumnDefinition:
    position: int
    name: str
    description: str
    type: ColumnType
    nullable: bool
    allow_duplicates: bool

    @staticmethod
    def _validate_payload(p: Dict[str, Any]) -> None:
        req = {"position", "name", "description", "type", "nullable", "allow_duplicates"}
        miss = req - p.keys()
        if miss:
            raise ValueError(f"Missing keys in column definition: {sorted(miss)}")
        if not isinstance(p["position"], int) or p["position"] < 1:
            raise ValueError("`position` must be a positive integer (1-based).")
        if p["type"] not in {"alphabetic", "integer", "date", "numeric"}:
            raise ValueError(f"Invalid `type`: {p['type']}")

    @classmethod
    def from_dict(cls, p: Dict[str, Any]) -> "ColumnDefinition":
        cls._validate_payload(p)
        return cls(
            position=p["position"],
            name=p["name"].strip(),
            description=p["description"].strip(),
            type=p["type"],
            nullable=bool(p["nullable"]),
            allow_duplicates=bool(p["allow_duplicates"])
        )
