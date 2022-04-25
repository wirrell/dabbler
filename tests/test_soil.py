import io
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pytest
import pandas as pd
from dabbler.soil import SoilGenerator, Soil
from shapely.geometry import Point


def load_soil_from_example_file(soil_code, coords):
    with open("test_data/example_soils/EX.SOL", "r") as f:
        soils = f.read()
    soils = soils.split("*")
    for soil in soils:
        if soil[:10] == soil_code:
            table = soil
            grid_code = table.split(" ")[0]
            sections = table.split("@")
            HC_code = "HC" + table.split("HC")[2][:8]
            properties = pd.read_csv(io.StringIO(sections[2]), sep="\s+")
            depth_table = pd.read_csv(io.StringIO(sections[3]), sep="\s+")
            return Soil(
                depth_table, properties, HC_code, Point(coords[::-1]), grid_code
            )


class TestSoilGenerator:
    @pytest.fixture
    def soil_generator(self):
        return SoilGenerator()

    def test_load_soildata(self, soil_generator):
        for code in soil_generator.HC27_soils:
            depth_table, properties = soil_generator.HC27_soils[code]
            assert isinstance(depth_table, pd.DataFrame)
            assert isinstance(properties, pd.DataFrame)

    @pytest.mark.parametrize(
        "soil_code,coords",
        [("US03072668", (30.708, -84.291)), ("US02532556", (41.125, -93.625))],
    )
    def test_generator_produces_soil_very_similar_to_DSSAT10KM_dataset(
        self, soil_generator, soil_code, coords
    ):
        ref_soil = load_soil_from_example_file(soil_code, coords)
        location = Point(coords[::-1])
        produced_soil = soil_generator.build_soils(
            {soil_code: location.buffer(0.001, cap_style=3)}
        )
        print(produced_soil)
        print(ref_soil)
