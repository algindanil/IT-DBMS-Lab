from pydantic import BaseModel, constr, create_model, validator
import pandas as pd

from typing import Dict, Type, Tuple
from datetime import datetime

from date_interval import DateInterval


type_map = {
    'string': str,
    'int': int,
    'float': float,
    'char': constr(max_length=1),
    'datetime': datetime,
    'dateinvl': Tuple[datetime, datetime],
}


def generate_schema(table_name: str, columns: Dict[str, str]) -> Type[BaseModel]:
    schema_fields = {}

    for col_name, col_type in columns.items():
        if col_type in type_map:
            schema_fields[col_name] = (type_map[col_type], ...)
        else:
            raise ValueError(f"Unknown type '{col_type}' for column '{col_name}'")

    model = create_model(table_name, **schema_fields)

    return model
