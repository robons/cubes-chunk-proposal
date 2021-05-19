from pathlib import Path
import json
import urllib
from typing import List, Dict
import re


from config import CsvCube, CsvCubeChunk, URI, CsvCubeConfig
from columns import CsvColumn, DimensionColumn, MeasureTypeColumn, AttributeColumn, ObservedValueColumn


def cube_to_csvw(cube: CsvCube, metadata_file_out: Path, suppress_dsd: bool = False):
    dataset_base_uri = get_url_relative_to_base(cube, f"data/{cube.config.dataset_identifier}")

    tables = []
    out_dir = metadata_file_out.parent
    for chunk in cube.get_chunks():
        _write_chunk_out(cube, chunk, out_dir, tables, dataset_base_uri)

    csvw_metadata = {
        "@context": "http://www.w3.org/ns/csvw",
        "@id": f"{dataset_base_uri}#dataset",
        "tables": tables,
        "rdfs:label": cube.config.catalog_metadata.title,
        "dc:title": cube.config.catalog_metadata.title,
        "rdfs:comment": cube.config.catalog_metadata.summary,
        "dc:description": cube.config.catalog_metadata.description
    }

    with open(metadata_file_out, "w+") as f:
        f.write(json.dumps(csvw_metadata, indent=4))


def _get_col_definition(col_title: str, cube_config: CsvCubeConfig, dataset_base_uri: URI) -> Dict:
    columns = cube_config.columns
    col_def = columns.get(col_title, DimensionColumn.new_dimension(col_title, None, None, None, None, None, None))
    csvw_col_name = re.sub("[^A-Za-z0-9]", "_", col_def.csv_column_title).lower()
    csvw_col_def = {
        "titles": col_def.csv_column_title,
        "name": csvw_col_name,
        "datatype": "string"  # default unless overridden
    }

    if isinstance(col_def, MeasureTypeColumn):
        if col_def.types is not None:
            csvw_col_def["datatype"] = {
                "base": "string",
                "format": f"^({'|'.join(col_def.types)})$"
            }
    if isinstance(col_def, DimensionColumn):
        csvw_col_def["propertyUrl"] = col_def.dimension_uri or f"{dataset_base_uri}#dimension/{csvw_col_name}"
        csvw_col_def["valueUrl"] = col_def.value_uri or f"{dataset_base_uri}#concept/{csvw_col_name}/{{+{csvw_col_name}}}"

    if isinstance(col_def, AttributeColumn):
        csvw_col_def["propertyUrl"] = col_def.attribute_uri
        csvw_col_def["valueUrl"] = col_def.value_uri

    if isinstance(col_def, ObservedValueColumn):
        csvw_col_def["propertyUrl"] = col_def.measure_uri
        if col_def.datatype is not None:
            csvw_col_def["datatype"] = col_def.datatype

    return csvw_col_def


def _write_chunk_out(cube: CsvCube, chunk: CsvCubeChunk, out_dir: Path, tables: List[dict], dataset_base_uri: URI):
    chunk_dir = out_dir / chunk.name
    if not chunk_dir.exists():
        chunk_dir.mkdir()
    chunk_dir_relative = chunk_dir.relative_to(out_dir)
    chunk_csv_name = f"{chunk.name}.csv"
    chunk_metadata_name = f"{chunk.name}.csv-metadata.json"
    chunk_table_schema_name = f"{chunk.name}.table.json"
    chunk.data.to_csv(out_dir / chunk.name / chunk_csv_name, index=False)
    tables.append({
        "url": str(chunk_dir_relative / chunk_csv_name),
        "tableSchema": str(chunk_dir_relative / chunk_table_schema_name)
    })

    chunk_table_schema = {
        "columns": [_get_col_definition(c, cube.config, dataset_base_uri) for c in chunk.data.columns]
    }
    with open(chunk_dir / chunk_table_schema_name, "w+") as f:
        f.write(json.dumps(chunk_table_schema, indent=4))
    chunk_csvw_metadata = {
        "@context": "http://www.w3.org/ns/csvw",
        "url": chunk_csv_name,
        "@id": get_url_relative_to_base(cube, f"data/{cube.config.dataset_identifier}#chunk/{chunk.name}"),
        "tableSchema": chunk_table_schema_name,
        "rdfs:label": chunk.name,
        "dc:title": chunk.name
    }
    with open(chunk_dir / chunk_metadata_name, "w+") as f:
        f.write(json.dumps(chunk_csvw_metadata, indent=4))


def get_url_relative_to_base(cube: CsvCube, relative_path: str) -> URI:
    return urllib.parse.urljoin(cube.config.base_uri + "/", relative_path)
