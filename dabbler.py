"""
dabbler - a simple Python wrapper for DSSAT.
"""
import os
import subprocess
import pandas as pd
import file_generator
from pathlib import Path
from io import StringIO


# TODO: add remaining DSSAT output files to Results class
# TODO: make it so that generated weather files are saved in the DSSAT
# weather dir

class Experiment:
    """Build and run a DSSAT experiment.

    Uses template experiment files and fills them in with the details.

    Parameters
    ----------
    EXP_ID : str
        Experiment identification code
    exp_location : str
        Location of experiment for header e.g. 'Somewheresville, FL'
    save_path : str
        Location to save results to
    dssat_install : str
        Path to the DSSAT install directory e.g. home/DSSAT/build/bin
    dssat_weather : str
        Path to the DSSAT weather file directory e.g. home/DSSAT/build/weather
    """
    # TODO: finish docstring

    implemented_models = ['MZIXM', '']
    implemented_crops = ['maize', 'wheat']

    def __init__(self, EXP_ID, exp_location, save_path, dssat_install,
                 dssat_weather):
        self.dssat_exe = self._check_install(dssat_install)
        if len(EXP_ID) != 4:
            raise ValueError('EXP_ID must be len 4 for DSSAT purposes.')
        self.EXP_ID = EXP_ID
        self.exp_location = exp_location
        self.save_path = save_path
        self.weather_path = dssat_weather

    def run(self, exp_filename=None, forecast=False, supress_stdout=True):
        """Run the experiment.

        Generates the EXP file, runs the experiment and returns the results.

        Parameters
        ----------
        exp_filename : str, optional
            If supplied, will used existing experiment file found at this path.
        forecast : bool, optional
            If true, will run DSSAT in the new forecast mode.
        supress_stdout : bool, optional
            If True, will supress the DSSAT Fortran stdout so it does not
            clog the terminal.

        Returns
        -------
        A results summary.
        """
        if forecast:
            run_mode = 'Y'  # this is the new forecast run mode
        else:
            run_mode = 'B'  # we run everything in through a batch file
            self.forecast_start = ''
            self.num_years = 1

        year = str(self.sim_start)[:2]
        save_name = f'{self.EXP_ID}{year}01' + self.template[-4:]
        if isinstance(exp_filename, type(None)):
            exp_filename = file_generator.generate_experiment(
                self.EXP_ID,
                self.plant_start,
                self.plant_end,
                self.harvest_date,
                self.harvest_date,
                self.harvest_date,
                self.sim_start,
                self.cultivar,
                self.soil_id,
                self.exp_location,
                self.save_path,
                save_name,
                self.template,
                self.forecast_start,
                self.num_years)

        # Generate the batch file so we can run in with subprocess
        batch_file = file_generator.generate_batchfile(exp_filename,
                                                       self.save_path,
                                                       self.crop.upper(),
                                                       self.EXP_ID)
        # OK - finally - open a subprocess to run DSSAT from 'within'
        # the simulation's save directory, so that the files are saved there.
        devnull = open(os.devnull, 'w')
        if supress_stdout:
            subprocess.run([self.dssat_exe, run_mode, batch_file],
                           cwd=self.save_path,
                           stdout=devnull)
        else:
            subprocess.run([self.dssat_exe, run_mode, batch_file],
                           cwd=self.save_path)

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

    def _set_model(self, model):
        # Check model has been implemented
        if model not in self.implemented_models:
            raise NotImplementedError(
                f'Have not yet implemented {model} for dabbler.'
            )
        return model

    def _set_crop(self, crop):
        # Check is crop has been implemented
        if crop.lower() not in self.implemented_crops:
            NotImplementedError(
                f'Have not yet implemented {crop} for dabbler.'
            )
        return crop

    def _check_install(self, dssat_install):
        install = Path(dssat_install)
        exe_loc = install.glob('dscsm047')
        try:
            exe = next(exe_loc)
        except StopIteration:
            raise RuntimeError('Could not find DSSAT exe in provided '
                               f'directory at {install}')
        return str(exe)

    def weather(self, weather, header=None):
        """Add weather data.

        Parameters
        ----------
        weather : pandas.DataFrame or str
            If str, should be path to existing weather file.
            Must have, at a minimum, columns '@DATE', 'TMAX', 'TMIN', 'RAIN'
            '@DATE' is in format YYDOY, temp in deg C, rain in mm.
        header : dict
            containing keyed header information (see file_generator for needs)

        Returns
        -------
        None
        """

        if isinstance(weather, str):
            self.weather_file = weather
            return
        try:
            year = str(self.sim_start)[:2]
        except AttributeError:
            raise RuntimeError('Set experiment timing before setting weather.')

        filename = f'{self.EXP_ID}{year}01.WTH'

        self.weather_file = file_generator.generate_weather(filename,
                                                            self.weather_path,
                                                            weather,
                                                            header)

    def phenology(self, crop, cultivar, model, template):
        """Add phenology information to experiment.

        Parameters
        ----------
        crop : str
            'Maize', 'Wheat', etc.
        cultivar : str
            Cultivar code taken from relevant .CUL file
        model : str
            Model to be used e.g. 'MZIXM' for CERES-IXIM
        template : str
            Path to the template experiemnt file to be used.

        Returns
        -------
        None
        """
        self.crop = self._set_crop(crop)
        self.cultivar = cultivar
        self.model = self._set_model(model)
        self.template = template

    def timing(self, sim_start, plant_start, plant_end, harvest_date):
        """Add timing information to experiment.

        Parameters
        ----------
        sim_start : int
            YYDOY
        plant_start : int
            YYDOY
        plant_end : int
            YYDOY
        harvest_date : int
            YYDOY

        Returns
        -------
        None
        """
        self.sim_start = sim_start
        self.plant_start = plant_start
        self.plant_end = plant_end
        self.harvest_date = harvest_date

    def soil(self, soil_id):
        """Add soil ID to experiment

        Parameters
        ----------
        soil_id : str
            Soil ID from .SOL file e.g. 'IB00000007'

        Returns
        -------
        None
        """
        self.soil_id = soil_id


class Results:
    # TODO: add in docstring that all tables are indexed by DOY
    # TODO: add that all properties are their equivalent DSSAT output filenames
    """Class to read in DSSAT results from .OUT files.

    Parameters
    ----------
    output_dir : str
        Location where DSSAT .OUT result files are.
    crop : str
        E.g. 'wheat', 'maize'
    clear_dir : bool, opt
        If true, delete files after loading them into this class.
    """

    file_layouts = {'wheat': {'PlantGro.OUT': 3,
                              'Evaluate.OUT': 4},
                    'maize': {'PlantGro.OUT': 5,
                              'Evaluate.OUT': 2}}

    def __init__(self, output_dir, crop, clear_dir=False):
        crop = crop.lower()
        self.output_dir = Path(output_dir)
        self.clear_dir = clear_dir
        self.Weather = self._load_table('Weather.OUT', 3)
        self.PlantGro = self._load_table(
            'PlantGro.OUT',
            self.file_layouts[crop]['PlantGro.OUT']
        )
        self.PlantN = self._load_table('PlantN.OUT', 3)
        self.ET = self._load_table('ET.OUT', 5)
        self.Evaluate = self._load_table(
            'Evaluate.OUT',
            self.file_layouts[crop]['Evaluate.OUT'],
            '@RUN'
        )
        self.Mulch = self._load_table('Mulch.OUT', 3)
        self.N2O = self._load_table('N2O.OUT', 6)
        self.SoilNBalSum = self._load_table('SoilNBalSum.OUT', 8, '@Run')
        self.SoilNi = self._load_table('SoilNi.OUT', 5)
        self.SoilTemp = self._load_table('SoilTemp.OUT', 7)
        self.SoilWat = self._load_table('SoilWat.OUT', 5)
        self._load_INFO()
        self._set_overview()

    def _load_table(self, table_name, skiprows=0, index='DOY', numrows=None):

        table_loc = self.output_dir.joinpath(table_name)
        try:
            table = pd.read_csv(table_loc, sep='\s+', skiprows=skiprows,  # noqa
                                nrows=numrows)
        except FileNotFoundError:
            return False  # This file wasn't computed for the DSSAT run
        # If index for the table is DOY then also generate a date column
        # and change the index itself to YYYYDDD so that simulations that span
        # more than one year are handled correctly down the line.
        # Otherwise, use specified index.
        if index == 'DOY':
            DOY_leading_zeroes = table['DOY'].apply('{:0>3}'.format)
            year_day = (table['@YEAR'].astype(str) +
                        DOY_leading_zeroes).astype(int)
            table.index = year_day
        else:
            table.index = table[index]

        if self.clear_dir:
            table_loc.unlink()

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
