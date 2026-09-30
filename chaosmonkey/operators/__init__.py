"""Operator adapters, looked up by the name used on the command line."""

from __future__ import annotations

from .altinity import AltinityOperator
from .base import OperatorAdapter
from .clickhouse import ClickHouseOperator

ADAPTERS: dict[str, type[OperatorAdapter]] = {
    AltinityOperator.name: AltinityOperator,
    ClickHouseOperator.name: ClickHouseOperator,
}


def get(name: str) -> type[OperatorAdapter]:
    try:
        return ADAPTERS[name]
    except KeyError:
        raise SystemExit(f"unknown operator {name!r}; known: {', '.join(sorted(ADAPTERS))}") from None
