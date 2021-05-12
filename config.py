from datetime import datetime
from typing import List, Dict, Optional, Any, Set, Iterable, NewType
import pandas as pd
from pathlib import Path
import json
import copy
from dateutil import parser


URI = NewType('URI', str)


def override_config_for_cube_id(config: dict, cube_id: str) -> Optional[dict]:
    """
        Apply cube config overrides contained inside the `cubes` dictionary to get the config for the given `cube_id` 
    """
    # Need to do a deep-clone of the config to avoid side-effecs
    config = copy.deepcopy(config)

    id = config.get("id")        
    if id is not None and id == cube_id:
        if "cubes" in config:
            del config["cubes"]

        return config
    elif "cubes" in config and cube_id in config["cubes"]:
        overrides = config["cubes"][cube_id]
        for k, v in overrides.items():
            config[k] = v

        return config
    else:
        return None


def get_from_dict_ensure_exists(config: dict, key: str) -> Any:
    val = config.get(key)
    if val is None:
        raise Exception(f"Couldn't find value for key '{key}'")
    return val


class CsvCubeCatalogMetadata:
    """
    Does any of this information need to be provided by a scraper?
    """

    title: str
    summary: Optional[str]
    description: Optional[str]
    creator: Optional[URI]
    publisher: Optional[URI]
    issued: datetime
    # modified: datetime # Should modified be in here or be automatically updated by the script?
    themes: List[str]
    keywords: List[str]
    landing_page: Optional[URI]
    license: Optional[URI]
    public_contact_point: Optional[URI]

    def __init__(self, d: dict):
        self.title = get_from_dict_ensure_exists(d, "title")
        self.summary = d.get("summary")
        self.description = d.get("description")
        self.creator = URI(d.get("creator"))
        self.publisher = URI(d.get("publisher"))
        self.issued = parser.parse(d.get("published"))
        self.themes = d.get("families")
        self.keywords = d.get("keywords")
        self.landing_page = d.get("landingPage")
        self.license = d.get("license")
        self.public_contact_point = d.get("contactUri")


class CsvCubeConfig:
    # Allow for override of base_uri
    base_uri: str
    dataset_identifier: str

    catalog_metadata: CsvCubeCatalogMetadata

    # Leave the columns nightmare for now.
    columns: List[dict]

    def __init__(self, d: dict):
        self.catalog_metadata = CsvCubeCatalogMetadata(d)

        self.dataset_identifier = get_from_dict_ensure_exists(d, "id")
        self.base_uri = d.get("baseUri", "http://gss-data.org.uk/")
        self.columns = d.get("columns", [])

    @staticmethod
    def from_info_json(info_json: Path, cube_id: str):
        with open(info_json, "r") as f:
            config = json.load(f)

        config = override_config_for_cube_id(config, cube_id)
        if config is None:
            raise Exception(f"Config not found for cube with id '{cube_id}'")
       
        return CsvCubeConfig(config)


class CsvCubeChunk:
    name: str
    data: pd.DataFrame

    def __init__(self, name: str, data: pd.DataFrame):
        self.name = name
        self.data = data


class CsvCube:
    config: CsvCubeConfig

    """
     Holds a map of chunk name to data.
     
     To support larger datasets, we could write this data to disk and re-load when 
    """
    _chunks: Set[CsvCubeChunk] = set()

    def __init__(self, config: CsvCubeConfig):
        self.config = config

    def set_data(self, data: pd.DataFrame, chunk_name: Optional[str] = None):
        if chunk_name is None:
            chunk_name = self.config.dataset_identifier

        if chunk_name in [c.name for c in self._chunks]:
            raise Exception(f"Chunk '{chunk_name}' has already been added.")

        self._chunks.add(CsvCubeChunk(chunk_name, data))

    def get_chunks(self) -> Iterable[CsvCubeChunk]:
        return self._chunks

    def deep_clone(self):
        return copy.deepcopy(self)