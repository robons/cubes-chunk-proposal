# Generate CSV boi

Key points:

* Don't need to specify graph URIs
* Trig file doesn't need to be generated anymore (catalog metadata contained withing the CSV-W file)

```python
from pathlib import Path
from config import CsvCube, CsvCubeConfig

config = CsvCubeConfig.from_info_json(Path("./info.json"), "hmrc-ots-cn8")
cube = CsvCube(config)

# Alternatively we could have something like
# cube = csvwtools.load_cube_from_csvw(Path(f"out/hmrc-ots-cn8.csv-metadata.json"))
# This would allow us to add some more data to an existing CSV-W.

# Do this for simple datasets:
cube.set_data(data_frame)

# Do this for chunked datasets (large):
cube.set_data(data_frame_2, chunk_name="Q2-2020")
cube.set_data(data_frame_3, chunk_name="Q3-2020")

# Qb-flavoured CSV-W output region
csvw_qb = cube.deep_clone()
for chunk in csvw_qb.get_chunks():
    # Transform the data to the format necessary for qb-flavoured CSV-W output
    chunk.data = chunk.data[chunk.data["Column"] == "value"]

# Call a separate tool which knows how to express the data & metadata in the CSV-W format
csvwtools.cube_to_csvw(csvw_qb, Path(f"./out/{cube.id}.csv-metadata.json"), include_dsd=False)

# CMD cube output region
cmd_cube = cube.deep_clone()

for chunk in cmd_cube.get_chunks():
    # Manipulate data to be in format suitable for CMD
    chunk.data = chunk.data[chunk.data["Column"] == "other_value"]

# Call a separate tool which knows how to express the data & metadata in the CMD format
# This tool should be in a separate module to improve extensibility in the future if other output formats become necessary.
# i.e. we don't couple together the representation and the output mechanism - CsvCube is agnostic to how it is output.
cmdtools.cube_to_cmd(cmd_cube, Path(f"./cmd-out/metadata.json"))
```

## Proposed info.json modifications

```json
{
    "id": "hmrc-ots",
    "baseUri": "http://gss-data.org.uk/trade/",
    "publisher": "HM Revenue & Customs",
    "landingPage": "https://www.uktradeinfo.com/trade-data/overseas/",
    "published": "2018-03-08",
    "license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
    "families": [
        "Trade"
    ],
    "keywords": [
        "Overseas",
        "Balance of Payments"
    ],
    "contactUri": "mailto:hmrc@hmrc.gov.uk",
    "cubes": {
        "hmrc-ots-cn8": {
            "title": "HMRC Overseas Trade Statistics - Combined Nomenclature 8",
            "summary": "HMRC OTS from the perspective of the Combined Nomenclature 8 codelist.",
            "description": "HMRC OTS from the perspective of the Combined Nomenclature 8 codelist... more information here."
        },
        "hmrc-ots-sitc": {
            "title": "HMRC Overseas Trade Statistics - SITCv4",
            "summary": "HMRC OTS from the perspective of the SITCv4 codelist.",
            "description": "HMRC OTS from the perspective of the SITCv4 codelist... more information here."
        }
    },
    "columns": {
```

* The base_uri property can be used to define the base for all of the URIs we generate. 
* We add all of the metadata necessary to generate appropriate catalogue metadata - this includes descriptions/comments!
* There's a new section called `cubes` which allows us to specify configuration options which should only be applied when generating a particular cube. This could solve the problems we've had where we have to manually alter the configuration inside the `main.py` file for a given cube. 

### Example of using cube-specific metadata

Calling `Cube(Path("./info.json"), "hmrc-ots-cn8")`, will yield a cube with the following metadata. Note that `title`/`summary`/`description` were specifies in the `cubes.hmrc-ots-cn8` property before.

```json
{
    "id": "hmrc-ots-cn8",
    "baseUri": "http://gss-data.org.uk/trade/",
    "publisher": "HM Revenue & Customs",
    "landingPage": "https://www.uktradeinfo.com/trade-data/overseas/",
    "published": "2018-03-08",
    "license": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
    "families": [
        "Trade"
    ],
    "keywords": [
        "Overseas",
        "Balance of Payments"
    ],
    "contactUri": "mailto:hmrc@hmrc.gov.uk",
    "title": "HMRC Overseas Trade Statistics - Combined Nomenclature 8",
    "summary": "HMRC OTS from the perspective of the Combined Nomenclature 8 codelist.",
    "description": "HMRC OTS from the perspective of the Combined Nomenclature 8 codelist... more information here.",
    "columns": {
```

## Output Directory Structure
```
.
├── hmrc-ots.csv-metadata.json
├── Q2-2020
│   ├── Q2-2020.csv
│   ├── Q2-2020.csv-metadata.json
│   └── Q2-2020.table.json
└── Q3-2020
    ├── Q3-2020.csv
    ├── Q3-2020.csv-metadata.json
    └── Q3-2020.table.json
```
The top-level file can be found in this case in [out/hmrc-ots.csv-metadata.json](out/hmrc-ots.csv-metadata.json). It references all chunks and contains the qb metadata (DSD, etc.). 

You'll notice that each chunk has its own folder (e.g. [out/Q2-2020](./out/Q2-2020)). This is so that we can create (and keep tidy) CSV-W metadata files for each individual chunk (e.g. [out/Q2-2020/Q2-2020.csv-metadata.json](./out/Q2-2020/Q2-2020.csv-metadata.json)). This is to ensure that we can run csv2rdf on each chunk individually (and potentially in parallel) to aide any future differential upload tool. Each chunk also has its own **table schema** file (e.g. [out/Q2-2020/Q2-2020.table.json](./out/Q2-2020/Q2-2020.table.json)) to ensure that the key column definitions can be referenced from the top-level CSV-W which defines and links all chunks into the dataset.

