"""dabbler test for Maize using Iowa Mesonet weather."""
import matplotlib.pyplot as plt
import matplotlib as mpl
from weather_tools import format_Iowa_mesonet
from dabbler import Experiment, Results

dssat_bin = '/home/george/DSSAT/build/bin'
dssat_weather = '/home/george/DSSAT/build/Weather'
iowa_mesonet = 'test_data/Iowa_Mesonet_all_2018_2019.txt'
test_data, test_header = format_Iowa_mesonet(iowa_mesonet, 'ESTHERVILLE-2-N',
                                             2018)
# Test experiment run
exp = Experiment('TEST', 'Test Location, TE', 'test/', dssat_bin, dssat_weather)
exp.phenology('Maize', 'PC0001', 'MZIXM', 'templates/template.MZX')
exp.timing(18001, 18100, 18110, 18300)
exp.weather(test_data, test_header)
exp.soil('IB00000007')
exp.run()

# Test results
results = Results('test/', 'Maize', clear_dir=True)
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
