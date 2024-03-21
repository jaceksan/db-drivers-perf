import os
from typing import Any, Optional, TypeVar

import attrs
import yaml
from cattrs import structure

T = TypeVar("T", bound="Base")


@attrs.define
class Base:
    @classmethod
    def from_dict(cls: type[T], data: dict[str, Any]) -> T:
        """
        Creates object from dictionary.
        """
        return structure(data, cls)

    def to_dict(self) -> dict[str, Any]:
        """
        Converts object into dictionary.
        """
        return attrs.asdict(self)


@attrs.define(auto_attribs=True, kw_only=True)
class BaseConfig(Base):
    query: str
    measurement_iterations: int


@attrs.define(auto_attribs=True, kw_only=True)
class Database(Base):
    name: str
    db_type: str
    host: str
    db_name: str
    user: str
    password: str
    odbc_driver_path: str
    port: Optional[str] = None
    account: Optional[str] = None  # Snowflake only
    warehouse: Optional[str] = None  # Snowflake only
    bottom_limit: Optional[int] = None
    top_limit: Optional[int] = None


@attrs.define(auto_attribs=True, kw_only=True)
class Location(Base):
    name: str
    databases: list[Database]


@attrs.define(auto_attribs=True, kw_only=True)
class Config(Base):
    config: BaseConfig
    locations: list[Location]


def read_config() -> Config:
    with open("config.yaml") as fp:
        config_dict = yaml.safe_load(fp)
        config = Config.from_dict(config_dict)

    for location in config.locations:
        for database in location.databases:
            database.password = os.getenv(database.password)
    return config
