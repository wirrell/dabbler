"""dabbler test for corn using two Iowa fields."""
import unittest
import os
import io
import sys
import json
import matplotlib.pyplot as plt
import matplotlib as mpl
from shapely.geometry import Polygon
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pathlib import Path

from dabbler import Experiment, Results
from dabbler.soil import SoilGenerator
import dabbler.weather

dssat_bin = '/home/george/DSSAT/build/bin'
dssat_weather = '/home/george/DSSAT/build/Weather'
dssat_soil = '/home/george/DSSAT/build/Soil'
test_run_loc = './test_run'


class TestUtilities(unittest.TestCase):


    def setUp(self):
        self.clear_test_dir()
        self.fields = self.load_test_fields()

    def clear_test_dir(self):
        test_dir = Path(test_run_loc)
        old_test_files = test_dir.glob('*')
        for old_test_file in old_test_files:
            old_test_file.unlink()

    def load_test_fields(self):
        fields = {}

        with open('test_data/field.geojson', 'r') as field_file:
            field_json = json.load(field_file)
        field = Polygon(field_json['features'][0]['geometry']['coordinates'][0])
        fields['TTST000001'] = field

        with open('test_data/field2.geojson', 'r') as field_file:
            field_json = json.load(field_file)
        field = Polygon(field_json['features'][0]['geometry']['coordinates'][0])
        fields['TTST000002'] = field

        return fields

    def test_SG_generate_soils(self):
        """Test dabbler.soil.SoilGenerator.generate_soils"""
        SG = SoilGenerator(dssat_soil)
        soil_file = SG.generate_soils(self.fields, './test_run', 'TT.SOL',
                                      test_run_loc)

        generated_soil_file_list = self.load_file_as_lines_list(soil_file)
        true_soil_file_list = self.load_file_as_lines_list('./test_data/TT.SOL')

        self.assertListEqual(
            generated_soil_file_list,
            true_soil_file_list
        )

    def load_file_as_lines_list(self, file_path):
        with io.open(file_path) as f:
            file_lines_list = list(f)
        return file_lines_list


    def test_generate_weather(self):
        """Test dabbler.weather.generate_weather."""
        for field in self.fields:
            weather_name = f'TTST{field[-4:]}.WTH' 
            weather = dabbler.weather.generate_weather(
                 [self.fields[field].centroid.xy[1][0],
                  self.fields[field].centroid.xy[0][0]],
                 2020,
                 field,
                 weather_name,
                 test_run_loc
            )

            generated_weather = self.load_file_as_lines_list(weather)
            true_weather = self.load_file_as_lines_list(f'./test_data/{weather_name}')

            self.assertListEqual(
                generated_weather,
                true_weather
            )

    def test_experiment_run(self):
        raise NotImplementedError
        pass
            

# Test experiment run
# for field in fields:
#     exp = Experiment('TEST', 'Test Location, TE', 'test_run/', dssat_bin, dssat_weather)
#     exp.phenology('Maize', 'PC0001', 'MZIXM', '../data/templates/template.MZX')
#     exp.timing(20001, 20100, 20110, 20300)
#     exp.weather(weather)
#     exp.soil(field)
#     exp.run()
# 
#     # Test results
#     results = exp.result
#     wsgd = results.PlantGro['WSGD']
#     wspd = results.PlantGro['WSPD']
#     lai = results.PlantGro['LAID']
#     rain = results.Weather['PRED']
# 
#     mpl.rcParams['font.size'] = 12
# 
#     fig, ax = plt.subplots()
#     ax2 = ax.twinx()
#     ax3 = ax.twinx()
#     ax2.set_ylim(0, 1)
# 
#     ax.bar(rain.index, rain, label='Rainfall (mm)')
#     wsgd.plot(ax=ax2, label='WSGD - Water stress expansion/development',
#               color='red')
#     wspd.plot(ax=ax2, label='WSPD - Water stress photosynthesis', color='orange')
#     lai.plot(ax=ax3, label='LAI', color='green', linewidth=3)
#     ax.set_ylabel('Rainfall (mm)')
#     ax.set_xlabel('DOY')
#     ax2.set_yticks([])
#     ax3.set_ylim(0, 5)
#     fig.legend()
#     plt.title('Rainfed DSSAT experiment - Estherville, IA')
#     plt.show()
