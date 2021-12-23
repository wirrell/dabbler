import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dabbler.soil import SoilGenerator
import pytest
import pandas as pd


class TestSoilGenerator:

    @pytest.fixture
    def soil_generator(self):
        return SoilGenerator()

    def test_load_soildata(self, soil_generator):
        for code in soil_generator.HC27_soils:
            depth_table, properties = soil_generator.HC27_soils[code]
            assert isinstance(depth_table, pd.DataFrame)
            assert isinstance(properties, pd.DataFrame)
