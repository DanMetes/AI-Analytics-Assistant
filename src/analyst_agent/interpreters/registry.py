from __future__ import annotations

from typing import Dict, Type

from .base import Interpreter
from .generic_tabular import GenericTabularInterpreter
from .orders_v1 import OrdersInterpreter
from .sales_v1 import SalesInterpreter


_REGISTRY: Dict[str, Type[Interpreter]] = {
    "generic_tabular": GenericTabularInterpreter,
    "orders_v1": OrdersInterpreter,
    "sales_v1": SalesInterpreter,
}


def get_interpreter(policy_name: str) -> Interpreter:
    cls = _REGISTRY.get(policy_name, GenericTabularInterpreter)
    return cls()
