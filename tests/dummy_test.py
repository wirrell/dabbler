import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pathlib import Path
from dabbler import DSSAT, Experiment
from datetime import date

dssat_bin = '/home/george/DSSAT/build/bin'
dssat_weather = '/home/george/DSSAT/build/Weather'
dssat_soil = '/home/george/DSSAT/build/Soil'

experiment = Experiment(
    crop='Maize',
    model='MZIXM',
    cultivar='PC0003',
    plant_date=date(1982, 2, 25),
    harvest_date=date(1982, 6, 25),
    simulation_start=date(1982, 1, 1),
    coordinates_latitude=29.6380,
    coordinates_longitude=-28.3689,
    weather_station_code='UFGA',
    soil_code='IBMZ910014'
)

dssat = DSSAT(dssat_bin, dssat_weather, dssat_soil)

results = dssat.run(experiment)

experiment = experiment._replace(plant_date = date(1982, 4, 25))
results = dssat.run(experiment)

print(results.PlantGro)
