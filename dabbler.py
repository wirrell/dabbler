"""
dabbler - a simple Python wrapper for DSSAT.
"""
import pandas as pd
from pathlib import Path


# TODO: add remaining DSSAT output files to Results class
# TODO: address the problem that comes from DSSAT appending to output files for
# subsequent runs rather than creating a new file.
# TODO: add handlers for INFO.OUT, OVERVIEW.OUT, SoilNiBal.OUT, SoilNoBal.out,
# SoilWatBal.OUT, WARNING.OUT (if needs be)



class Results:
    # TODO: add in docstring that all tables are indexed by DOY
    # TODO: add that all properties are their equivalent DSSAT output filenames

    def __init__(self, output_dir):
        self.output_dir = Path(output_dir)
        self.Weather = self._load_table('Weather.OUT', 3)
        self.PlantGro = self._load_table('PlantGro.OUT', 5)
        self.PlantN = self._load_table('PlantN.OUT', 3)
        self.ET = self._load_table('ET.OUT', 5)
        self.Evaluate = self._load_table('Evaluate.OUT', 2, '@RUN')
        self.Mulch = self._load_table('Mulch.OUT', 3)
        self.N2O = self._load_table('N2O.OUT', 6)
        self.SoilNBalSum = self._load_table('SoilNBalSum.OUT', 8, '@Run')
        self.SoilNi = self._load_table('SoilNi.OUT', 5)
        self.SoilTemp = self._load_table('SoilTemp.OUT', 7)
        self.SoilWat = self._load_table('SoilWat.OUT', 5)
        self.Summary = self._load_table('Summary.OUT', 3, 'RUNNO')

    def _load_table(self, table_name, skiprows=0, index='DOY'):

        table_loc = self.output_dir.joinpath(table_name)
        table = pd.read_csv(table_loc, sep='\s+', skiprows=skiprows)
        table.index = table[index]

        return table



if __name__ == '__main__':
    dssat_bin = '/home/george/DSSAT/build/bin'
    results = Results(dssat_bin)
    wsgd = results.PlantGro['WSGD']
    wspd = results.PlantGro['WSPD']
    lai = results.PlantGro['LAID']
    rain = results.Weather['PRED']

    print(results.N2O)
    exit()

    import matplotlib.pyplot as plt
    import matplotlib as mpl
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
    ax3.set_ylim(0, 3)
    fig.legend()
    plt.title('Rainfed DSSAT experiment - Maize UFGA8201.MZX')
    plt.show()
