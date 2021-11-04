# dabbler - a python wrapper for the DSSAT crop modelling system.


## Quickstart Example
```
	dssat_bin = '/path/to/DSSAT/build/bin'
	exp = Experiment('TEST', 'Test Location, TE', 'test/', dssat_bin, dssat_weather)
	exp.phenology('Maize', 'PC0001', 'MZIXM', 'templates/template.MZX')  
	exp.timing(18001, 18100, 18110, 18300)
	exp.weather('WTHR')  # four letter weather station code
	exp.soil('IB00000007')  # generic soil ID
	exp.run()
```

