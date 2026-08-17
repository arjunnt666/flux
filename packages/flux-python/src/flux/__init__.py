"""Flux Python helpers for event envelope JSON."""
from __future__ import annotations
from typing import Any, Dict, List

def fold_balance(events: List[Dict[str, Any]]) -> int:
    bal = 0
    for e in events:
        t = e.get("type_name") or e.get("type")
        data = e.get("data") or {}
        if t == "Deposited":
            bal += int(data.get("amount", 0))
        elif t == "Withdrawn":
            bal -= int(data.get("amount", 0))
        elif t == "Opened":
            bal = 0
    return bal

def version() -> str:
    return "0.1.0"

__all__ = ["fold_balance", "version"]
