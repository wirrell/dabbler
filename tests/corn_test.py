"""dabbler test for Maize using Iowa Mesonet weather."""
import os
import sys
import json
import matplotlib.pyplot as plt
import matplotlib as mpl
from shapely.geometry import Polygon
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dabbler import Experiment, Results
from dabbler.soil import SoilGenerator
from dabbler.weather import generate_weather

dssat_bin = '/home/george/DSSAT/build/bin'
dssat_weather = '/home/george/DSSAT/build/Weather'
dssat_soil = '/home/george/DSSAT/build/Soil'

test_run_loc = './test_run'

# Load in test fields
fields = {}
with open('test_data/field.geojson', 'r') as field_file:
    field_json = json.load(field_file)
field = Polygon(field_json['features'][0]['geometry']['coordinates'][0])
fields['TTST000001'] = field
with open('test_data/field2.geojson', 'r') as field_file:
    field_json = json.load(field_file)
field = Polygon(field_json['features'][0]['geometry']['coordinates'][0])
fields['TTST000002'] = field

# Generate a soil file
SG = SoilGenerator(dssat_soil)
SG.generate_soils(fields, 'test_loc', 'TT.SOL', test_run_loc)

# Test experiment run
for field in fields:
    weather = generate_weather(
        [fields[field].centroid.xy[1][0], fields[field].centroid.xy[0][0]],
        2020,
        field,
        'TEST2001.WTH',
        test_run_loc
    )
    exp = Experiment('TEST', 'Test Location, TE', 'test_run/', dssat_bin, dssat_weather)
    exp.phenology('Maize', 'PC0001', 'MZIXM', '../data/templates/template.MZX')
    exp.timing(20001, 20100, 20110, 20300)
    exp.weather(weather)
    exp.soil('IB00000007')
    exp.run()

    # Test results
    results = Results('test_run/', 'Maize', clear_dir=True)
    wsgd = results.PlantGro['WSGD']
    wspd = results.PlantGro['WSPD']
    lai = results.PlantGro['LAID']
    rain = results.Weather['PRED']

    mpl.rcParams['font.size'] = 12

    fig, ax = plt.subplots()
    ax2 = ax.twinx()
    ax3 = ax.twinx()
    ax2.set_ylim(0, 1)

    ax.bar(rain.index, rain, label='Rainfall (mm)')
    wsgd.plot(ax=ax2, label='WSGD - Water stress expansion/development',
              color='red')
    wspd.plot(ax=ax2, label='WSPD - Water stress photosynthesis', color='orange')
    lai.plot(ax=ax3, label='LAI', color='green', linewidth=3)
    ax.set_ylabel('Rainfall (mm)')
    ax.set_xlabel('DOY')
    ax2.set_yticks([])
    ax3.set_ylim(0, 5)
    fig.legend()
    plt.title('Rainfed DSSAT experiment - Estherville, IA')
    plt.show()
