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
        "A": [1, 1, 1],
        "B": [2, 2, 2]
    }
), "Q2-2020")

hmrc_ots_cn8_cube.set_data(pd.DataFrame({
    "A": [3, 3],
    "B": [4, 4]
}), chunk_name="Q3-2020")

csvwtools.cube_to_csvw(hmrc_ots_cn8_cube, Path(f"out/{hmrc_ots_cn8_cube.config.dataset_identifier}.csv-metadata.json"))
