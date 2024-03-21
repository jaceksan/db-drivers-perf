from typing import Optional

import attrs
from python_libs.config import Base


@attrs.define(auto_attribs=True, kw_only=True)
class PythonResult(Base):
    limit: int
    location: str
    database: str
    connection_type: str
    durations: list[float]
    avg_duration: float
    error: Optional[str]


@attrs.define(auto_attribs=True, kw_only=True)
class PythonResults(Base):
    results: list[PythonResult]


@attrs.define(auto_attribs=True, kw_only=True)
class JavaResultParams(Base):
    connectionType: str
    dbType: str
    limit: str


@attrs.define(auto_attribs=True, kw_only=True)
class JavaResultMetric(Base):
    rawData: list[list[float]]


@attrs.define(auto_attribs=True, kw_only=True)
class JavaResult(Base):
    params: JavaResultParams
    primaryMetric: JavaResultMetric
