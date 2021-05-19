from pathlib import Path

import pandas as pd

from config import CsvCube, CsvCubeConfig
import csvwtools


hmrc_ots_cn8_config = CsvCubeConfig.from_info_json(Path("info.json"), "hmrc-ots-cn8")
hmrc_ots_cn8_cube = CsvCube(hmrc_ots_cn8_config)

# Alternatively we could have something like
# hmrc_ots_cn8_cube = csvwtools.load_cube_from_csvw(Path(f"out/{hmrc_ots_cn8_cube.config.dataset_identifier}.csv-metadata.json"))
# This would allow us to add some more data to an existing CSV-W.


hmrc_ots_cn8_cube.set_data(pd.DataFrame(
    {
        "flow_type": [1, 1, 1],
        "country_id": [2, 2, 2],
        "sitc_id": [3, 3, 3],
        "cn8_id": [4, 4, 4],
        "port": [5, 5, 5],
        "period": [6, 6, 6],
        "measure_type": ["net-mass", "net-mass", "monetary-value"],
        "unit_type": [8, 8, 8],
        "value": [9.0, 9.0, 9.0]
    }
), "Q2-2020")

hmrc_ots_cn8_cube.set_data(pd.DataFrame({
        "flow_type": [-1, -1, -1],
        "country_id": [-2, -2, -2],
        "sitc_id": [-3, -3, -3],
        "cn8_id": [-4, -4, -4],
        "port": [-5, -5, -5],
        "period": [-6, -6, -6],
        "measure_type": ["net-mass", "net-mass", "monetary-value"],
        "unit_type": [-8, -8, -8],
        "value": [-9.0, -9.0, -9.0]
    }
), chunk_name="Q3-2020")

csvwtools.cube_to_csvw(hmrc_ots_cn8_cube, Path(f"out/{hmrc_ots_cn8_cube.config.dataset_identifier}.csv-metadata.json"))
