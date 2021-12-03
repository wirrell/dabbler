# dabbler - a python wrapper for the DSSAT crop modelling system.


## Quickstart Example
```
	from dabbler import DSSAT, Experiment
	from datetime import date

	dssat_bin = '/path/to/DSSAT/build/bin'
	dssat_weather = '/path/to/DSSAT/build/Weather'
	dssat_soil = '/path/to/DSSAT/build/Soil'

	dssat = DSSAT(dssat_bin, dssat_weather, dssat_soil)

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

	results = dssat.run(experiment)

```
