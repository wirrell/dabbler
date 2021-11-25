"""
dabbler - a simple Python wrapper for DSSAT.
"""
import os
import atexit
import select
import subprocess
import pandas as pd
import pathlib
import datetime
from . import file_generator
from . import templates
from threading import Thread
from typing import Union
from pathlib import Path
from io import StringIO
from typing import NamedTuple


# TODO: change DSSAT to enable writing to FIFOs so that OPEN doesn't cause illegal seek
# See: https://stackoverflow.com/questions/11780556/write-to-fifo-named-pipe
# NOTE: this may also make it possible to uncomment the commented out files in DSSAT_OUT_FILES


class DSSAT:
    """Class the represents the DSSAT executable.

    Parameters
    ----------
    dssat_install : str
        Path to the DSSAT install directory e.g. home/DSSAT/build/bin
    dssat_weather : str
        Path to the DSSAT weather file directory e.g. home/DSSAT/build/weather
    """
    # NOTE: files commented out are files that DSSAT regularly reads from itself.
    # Get cannot FIFO them as the result dissappears once we read from the FIFO
    DSSAT_OUT_FILES = [
        # 'ERROR.OUT',
        'ET.OUT',
        'Evaluate.OUT',
        # 'INFO.OUT',
        # 'LUN.LST',
        'Mulch.OUT',
        'N2O.OUT',
        'OVERVIEW.OUT',
        'PlantGro.OUT',
        'PlantN.OUT',
        'RunList.OUT',
        'SoilNBalSum.OUT',
        'SoilNiBal.OUT',
        'SoilNi.OUT',
        'SoilNoBal.OUT',
        'SoilTemp.OUT',
        'SoilWatBal.OUT',
        'SoilWat.OUT',
        'Summary.OUT',
        # 'WARNING.OUT',
        'Weather.OUT'
    ]

    # Have to format fifos for weathe soil inputs as it
    # must reside in the /build/Weather
    DSSAT_IN_FILES = {
        'EXP': 'PIPE0001.EXP',
        'WTH': 'PIPE{pid}.WTH',
        'BATCH': 'BATCH.v47'
    }
    # No fifos used for soil.


    def __init__(self, dssat_install, dssat_weather, dssat_soil, run_location=Path.cwd()):
        self.dssat_exe = self._check_install(dssat_install)
        self.dssat_weather = Path(dssat_weather)
        self.dssat_soil = Path(dssat_soil)
        self.create_in_out_location()
        self.build_fifos()

    def create_in_out_location(self):
        pid = os.getpid()
        in_out_location = Path.cwd() / f'DSSAT_IO_{pid}'
        in_out_location.mkdir(exist_ok=True)
        self.in_out_location = in_out_location
        atexit.register(self.clean_in_out_on_exit)

    def clean_in_out_on_exit(self):
        for io_file in self.in_out_location.glob('*'):
            io_file.unlink()
        self.in_out_location.rmdir()

    def build_fifos(self):
        """Builds the fifos used to interface with DSSAT."""
        self.build_in_fifos()
        self.build_out_fifos()

    def build_out_fifos(self):
        # DSSAT will automatically append to them
        self.out_fifos = {}
        for out_file in self.DSSAT_OUT_FILES:
            out_fifo = self.in_out_location / out_file
            os.mkfifo(out_fifo)
            self.out_fifos[out_file] = out_fifo

    def build_in_fifos(self):
        self.in_fifos = {}
        pid = os.getpid()
        exp_fifo = self.in_out_location / self.DSSAT_IN_FILES['EXP']
        # os.mkfifo(exp_fifo) EXP cant be fifo as DSSAT uses rewind multiple times
        self.in_fifos['EXP'] = exp_fifo
        wth_fifo = self.dssat_weather / self.DSSAT_IN_FILES['WTH'].format(
            pid=str(pid)[-4:]
        )
        os.mkfifo(wth_fifo)
        self.in_fifos['WTH'] = wth_fifo
        batch_fifo = self.in_out_location / self.DSSAT_IN_FILES['BATCH']
        os.mkfifo(batch_fifo)
        self.in_fifos['BATCH'] = batch_fifo

    def run(self, experiment, supress_stdout=True):
        """Run the passed experiment.

        Returns
        -------
        dabbler.Results 
        """
        weather_file_string = None
        if experiment.weather_station_code is None:
            weather_file_string = file_generator.generate_weather_file_string(
                experiment
        )
            experiment.weather_station_code = self.in_fifos['WTH'].stem

        if experiment.soil_code is None:
            raise NotImplementedError('Have not yet implemented soil file writing.')

        experiment_file_string = file_generator.generate_experiment_file_string(experiment)

        # Generate the batch file so we can run in with subprocess
        batch_file_string = file_generator.generate_batchfile_string(experiment,
                                                              self.in_fifos['EXP'])

        # Deploy write threads to wait for DSSAT read by themselves
        self.deploy_write_threads(weather_file_string,
                                  experiment_file_string,
                                  batch_file_string)

        # Instance the Results class. It will spawn the read threads
        result = Results(self.out_fifos, experiment.crop)

        # OK - finally - open a subprocess to run DSSAT from 'within'
        # the simulation's save directory, so that the files are saved there.
        supress_stdout = False
        if supress_stdout:
            devnull = open(os.devnull, 'w')
            subprocess.Popen([self.dssat_exe, 'A', self.in_fifos['BATCH'].name],
                           cwd=self.in_out_location,
                           stdout=devnull)
        else:
            subprocess.Popen([self.dssat_exe, 'A', self.in_fifos['BATCH'].name],
                           cwd=self.in_out_location)

        # Tell the Results object to join read threads now we have run DSSAT
        result.read_outputs()

        print(result.PlantGro)

    def deploy_write_threads(self, weather_string, experiment_string, batch_string):
        # Send threads off to write to fifos as soon as DSSAT tries to read from them
        write_threads = []
        if weather_string is not None:
            write_threads.append(Thread(target=self.write_string_to_fifo,
                                        args=(weather_string, self.in_fifos['WTH'],)))
        write_threads.append(Thread(target=self.write_string_to_fifo,
                                    args=(experiment_string, self.in_fifos['EXP'],)))
        write_threads.append(Thread(target=self.write_string_to_fifo,
                                    args=(batch_string, self.in_fifos['BATCH'],)))
        [thread.start() for thread in write_threads]
        return write_threads

    def write_string_to_fifo(self, string, fifo):
        # Blocks until DSSAT goes to read from the fifo
        with open(fifo, 'w') as f:
            f.write(string)

    def read_from_fifo(self, fifo):
        # Blocks until DSSAT writes to fifo
        with open(fifo, 'r') as f:
            output = f.read()
        return output

    def forecast(self, forecast_start, num_years):
        """Define forecasting parameters for a forecast run.

        Parameters
        ----------
        forecast_start : int
            Must be full form (YYYYDDD) for a forecast run.
        num_years : int
            Total numbers of years to pull from historical weather for the run.

        Returns
        -------
        None
        """
        if len(str(forecast_start)) != 7:
            raise ValueError('Forecast start date must be full form YYYYDDD.')

        self.forecast_start = forecast_start
        self.num_years = num_years

    def _check_install(self, dssat_install):
        install = Path(dssat_install)
        exe_loc = install.glob('dscsm047')
        try:
            exe = next(exe_loc)
        except StopIteration:
            raise RuntimeError('Could not find DSSAT exe in provided '
                               f'directory at {install}')
        return str(exe)


class Experiment(NamedTuple):
    
    crop: str
    model: str
    cultivar: str
    plant_date: datetime.date
    harvest_date: datetime.date
    simulation_start: datetime.date
    coordinates_latitude: float
    coordinates_longitude: float
    soil_data: pd.DataFrame = None
    soil_code: str = None
    weather_data: pd.DataFrame = None
    weather_station_code: str = None
    elevation: int = None
    average_soil_temperature: float = None
    average_soil_temp_amplitude: float = None
    weather_measurements_refernce_height: float = None
    wind_measurements_refernce_height: float = None
    plant_end: datetime.date = None
    harvest_end: datetime.date = None
    experiment_ID: str = 'DFLT'
    experiment_location_name: str = 'DFLT'
    results_savelocation: str = None
    forecast_from_date: datetime.date = None
    num_forecast_years: int = None


class Results:
    """Class to read in and format DSSAT results from fifos.

    Parameters
    ----------
    output_fifos
        Location of output fifos that DSSAT will write to.
    crop : str
        E.g. 'wheat', 'maize'
    """

    # Outfile layouts by crop. Number is rows to skip.
    # If number is None, we just read from the output and trash it
    file_layouts = {'maize': {'ERROR.OUT': None,
                              'ET.OUT': 5,
                              'Evaluate.OUT': 2,
                              'INFO.OUT': None,
                              'LUN.LST': None,
                              'Mulch.OUT': 3,
                              'N2O.OUT': 6,
                              'OVERVIEW.OUT': None,
                              'PlantGro.OUT': 5,
                              'PlantN.OUT': 3,
                              'RunList.OUT': None,
                              'SoilNBalSum.OUT': 8,
                              'SoilNiBal.OUT': None,
                              'SoilNi.OUT': 5,
                              'SoilNoBal.OUT': None,
                              'SoilTemp.OUT': 7,
                              'SoilWatBal.OUT': None,
                              'SoilWat.OUT': 5,
                              'Summary.OUT': None,
                              'WARNING.OUT': None,
                              'Weather.OUT': 3}
                   }


    def __init__(self, output_fifos, crop):
        self.output_fifos = output_fifos
        self.crop = crop.lower()
        self.read_threads = self.start_read_threads()

    def start_read_threads(self):
        read_threads = {}
        for fifo_name in self.output_fifos:
            read_threads[fifo_name] = Thread(
                target=self._load_table,
                args=(self.output_fifos[fifo_name],
                self.file_layouts[self.crop][fifo_name])
            )
            read_threads[fifo_name].start()
        return read_threads

    def read_outputs(self):
        results = self.get_results_from_read_threads(self.read_threads)
        # Set results tables as attributes of object
        for result in results:
            setattr(self, result, results[result])

    def get_results_from_read_threads(self, read_threads):
        results = {}
        for fifo_name in read_threads:
            results[fifo_name] = read_threads[fifo_name].join()
        return results

    def _load_table(self, fifo_loc, skiprows=0, index='DOY', numrows=None):

        with open(fifo_loc, 'r') as fifo:
            while True:
                select.select([fifo],[],[fifo])
                out_string = fifo.read()

        if skiprows is None:
            return None

        table = pd.read_csv(StringIO(out_string), sep='\s+', skiprows=skiprows,  # noqa
                            nrows=numrows)
        if index == 'DOY':
            DOY_leading_zeroes = table['DOY'].apply('{:0>3}'.format)
            year_day = (table['@YEAR'].astype(str) +
                        DOY_leading_zeroes).astype(int)
            table.index = year_day
        else:
            table.index = table[index]

        return table

    def _load_INFO(self):
        # Load INFO.OUT file special case
        # TODO: lift other information from this file as needed
        table_loc = self.output_dir.joinpath('INFO.OUT')

        # TODO: write splitter here to get fileshape independent
        # soil table extraction
        # Load soil information in
        # NOTE: consider splitting string on CLAY SILT SOIL keywords
        try:
            with open(table_loc, 'r') as f:
                table_string = f.read().split('Soil ID')[1]
        except FileNotFoundError:
            return  # No info file generated
        table = pd.read_csv(StringIO(table_string), sep='\s+',  # noqa
                            skiprows=6,
                            nrows=10)
        table['Depth'] = [x[1] for x in table.index]
        table = table.drop(table.index[0])
        table.index = [int(x) for x in table.index.levels[0][:-1]]
        self.SoilInfo = table

    def _set_overview(self):
        # Read in the overview file and assign it to the object.
        try:
            with open(self.output_dir.joinpath('OVERVIEW.OUT'), 'rb') as f:
                overview = f.read().decode('unicode_escape')
        except FileNotFoundError:
            return  # No overview file generated
        self.overview = overview
        with open(self.output_dir.joinpath('OVERVIEW.OUT'), 'rb') as f:
            crop_info = f.readlines()[11].decode('unicode_escape')
        self.crop_info = crop_info

        overview_sections = overview.split('*')
        for x in overview_sections:
            if 'SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES' \
               in x:
                # Need to format the table for easy pandas load
                def add_space(line): return line[:12] + ' ' + line[12:]
                lines = x.split('\n')
                lines = map(add_space, lines)
                x = '\n'.join(lines)
                x = StringIO(x)
                growth_stage_table = pd.read_csv(x, sep='\s\s+',  # noqa
                                                 skiprows=7,
                                                 skipinitialspace=True,
                                                 names=['Growth Stage',
                                                        'GSTD_code'],
                                                 usecols=[2, 12],
                                                 engine='python')
                growth_stage_table['Start'] = None
                growth_stage_table['End'] = None
                for index, row in growth_stage_table.iterrows():
                    try:
                        gstd = self.PlantGro[
                            self.PlantGro['GSTD'] == row['GSTD_code']]
                        start = gstd.index[0]
                        end = gstd.index[-1]
                    except IndexError:  # No row with this stage
                        continue
                    growth_stage_table.loc[index, 'Start'] = start
                    growth_stage_table.loc[index, 'End'] = end
                growth_stage_table.index = growth_stage_table['GSTD_code']
                self.GrowthTable = growth_stage_table

    def __str__(self):
        return self.overview
