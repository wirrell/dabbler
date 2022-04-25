# dabbler - a python wrapper for the DSSAT crop modelling system.

## Description
This project was started to make running DSSAT for the nth time a little more convenient.

### How it does this:

- EXP and WTH files are built using templates and parameters passed in through python.
- Results (PlantGro.OUT, ET.OUT, SoilTemp.OUT etc.) are read straight into memory 
- Soil files can be generated for anywhere in the world from SoilGrids data using dabbler.soil.SoilGenerator


### The Caveats

1. **dabbler** is currently only designed for using the DSSAT CERES-Maize and CERES-IXIM-Maize models (corn).
2. To use **dabbler**, you will need to compile a modified version on the latest version of DSSAT, found [here](https://github.com/wirrell/dssat-csm-os)

### Why do I need to use a modified version of DSSAT?
dabbler uses FIFOs to trick DSSAT into writing directly to buffers, avoiding unnecessary overhead so that things can keep churning. To do this, we need to use modified FORTRAN `OPEN` statements to use `ACCESS='STREAM'`. The forker DSSAT repository linked above contains a version with these modifications made in the DSSAT IO modules.



## Quickstart Example
```
	from dabbler import DSSAT, Experiment
	from datetime import date

	dssat_bin = '/path/to/DSSAT/build/bin'
	dssat_soil = '/path/to/DSSAT/build/Soil'

	dssat = DSSAT(dssat_bin, dssat_soil)

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

	# See plant growth table
	print(results.PlantGro)

```
