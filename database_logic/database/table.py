from pydantic import BaseModel, fields
import pandas as pd

from datetime import datetime
from typing import List, Dict, Tuple

from date_interval import DateInterval


class Table:
    def __init__(self, name, schema: BaseModel):
        self.name = name
        self.schema = schema
        self._dataframe = self._init_dataframe()

    def _init_dataframe(self):
        cols = list(self.schema.model_fields.keys())
        dtypes = self.validate_types_for_pd_format(list(self.schema.model_fields.values()))
        return pd.DataFrame({col_name: pd.Series(dtype=dt) for col_name, dt in zip(cols, dtypes)})

    @staticmethod
    def validate_types_for_pd_format(types: List[fields.FieldInfo]):
        pd_types = []
        for t in types:
            if t.annotation == str:
                pd_types.append('object')
            elif t.annotation == int:
                pd_types.append('int64')
            elif t.annotation == float:
                pd_types.append('float64')
            elif t.annotation == datetime:
                pd_types.append('datetime64[ns]')
            elif t.annotation == Tuple[datetime, datetime]:
                pd_types.append('interval')
            else:
                raise ValueError(f"Unknown type '{t.annotation}'")
        return pd_types

    def insert(self, row: Dict):
        try:
            validated_row = self.schema(**row)
        except Exception as e:
            raise ValueError(f"Validation failed: {e}")
        self._dataframe = pd.concat([self._dataframe, pd.DataFrame(validated_row)])
        # self._dataframe = self._dataframe.reset_index()

    def read_by_index(self, index: int) -> BaseModel:
        try:
            return self._dataframe.iloc[index]
        except IndexError:
            raise ValueError(f"Index {index} out of range")

    def update(self, index: int, new_data: Dict):
        try:
            validated_row = self.schema(**new_data)
        except Exception as e:
            raise ValueError(f"Validation failed: {e}")
        self._dataframe.iloc[index] = pd.Series(validated_row)
        self._dataframe = self._dataframe.reset_index()

    def delete(self, index: int):
        self._dataframe = self._dataframe.drop([index])

    def drop_duplicates(self, subset: List[str] = None):
        if subset:
            try:
                self._dataframe = self._dataframe.drop_duplicates(subset=subset)
            except KeyError as e:
                raise ValueError(f"Column {e} not found")
        else:
            self._dataframe = self._dataframe.drop_duplicates()

    def run_query(self, query: str) -> pd.DataFrame:
        try:
            return self._dataframe.query(query)
        except Exception as e:
            raise ValueError(f"Query failed: {e}")
