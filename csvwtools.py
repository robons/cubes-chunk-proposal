from pathlib import Path
import json
import urllib
from typing import List

from config import CsvCube, CsvCubeChunk, URI


def cube_to_csvw(cube: CsvCube, metadata_file_out: Path, suppress_dsd: bool = False):
    tables = []
    out_dir = metadata_file_out.parent
    for chunk in cube.get_chunks():
        write_chunk_out(cube, chunk, out_dir, tables)

    csvw_metadata = {
        "@context": "http://www.w3.org/ns/csvw",
        "@id": get_url_relative_to_base(cube, f"data/{cube.config.dataset_identifier}#dataset"),
        "tables": tables,
        "rdfs:label": cube.config.catalog_metadata.title,
        "dc:title": cube.config.catalog_metadata.title,
        "rdfs:comment": cube.config.catalog_metadata.summary,
        "dc:description": cube.config.catalog_metadata.description
    }

    with open(metadata_file_out, "w+") as f:
        f.write(json.dumps(csvw_metadata, indent=4))


def write_chunk_out(cube: CsvCube, chunk: CsvCubeChunk, out_dir: Path, tables: List[dict]):
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
        "columns": []
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
