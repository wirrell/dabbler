"""
dabbler - a simple Python wrapper for DSSAT.
"""
import os
import sys
import signal
import queue
import logging
import time
import atexit
import select
import subprocess
import pandas as pd
import datetime
from . import file_generator
from . import soil
from .dabbler_errors import SimulationFailedError
from threading import Thread
from pathlib import Path
from io import StringIO
from typing import NamedTuple


# TODO: add track_nitrogen and track_water variables to Experiment, then use
# that to select which fifos are built (e.g. SoilNBalSum.OUT is not used when
# no N tracked)


class DSSAT:
    """Class the represents the DSSAT executable.

    Parameters
    ----------
    dssat_install : str
        Path to the DSSAT install directory e.g. home/DSSAT/build/bin
    dssat_weather : str
        Path to the DSSAT weather file directory e.g. home/DSSAT/build/weather
    """

    # NOTE: files commented out are files that DSSAT regularly reads from
    # itself. We cannot FIFO them as the result dissappears once we read
    # from the FIFO
    DSSAT_OUT_FILES = [
        # 'ERROR.OUT',
        "ET.OUT",
        "Evaluate.OUT",
        # 'INFO.OUT',
        # 'LUN.LST',
        "Mulch.OUT",
        # 'N2O.OUT', NOTE: not currently used in our applications
        # 'OVERVIEW.OUT', NOTE: skip overview for now as it requires bytes read
        "PlantGro.OUT",
        "PlantN.OUT",
        # "RunList.OUT", NOTE: RunList causes read blocking after a prior failed run
        # 'SoilNBalSum.OUT',                 DSSAT may leave it open.
        # 'SoilNiBal.OUT',
        # 'SoilNi.OUT',
        # 'SoilNoBal.OUT',
        "SoilTemp.OUT",
        "SoilWatBal.OUT",
        "SoilWat.OUT",
        "Summary.OUT",
        # 'WARNING.OUT',
        "Weather.OUT",
    ]

    # Have to format fifos for weathe soil inputs as it
    # must reside in the /build/Weather
    DSSAT_IN_FILES = {
        "EXP": "EXPT0001.EXP",
        "WTH": "WTHR{pid}.WTH",
        "SOIL": "SOIL.SOL",
        "BATCH": "BTCH{pid}.v47",
    }
    # No fifos used for soil.

    def __init__(self, dssat_install, dssat_soil, run_location=Path.cwd()):
        self.dssat_exe = self._check_install(dssat_install)
        self.dssat_soil = Path(dssat_soil)
        self.create_in_out_location()
        self.build_fifos()

    def create_in_out_location(self):
        pid = os.getpid()
        in_out_location = Path.cwd() / f"DSSAT_IO_{pid}"
        in_out_location.mkdir(exist_ok=True)
        self.in_out_location = in_out_location
        atexit.register(self.clean_in_out_on_exit)
        # Use signal to catch multi-process exit
        signal.signal(signal.SIGTERM, self.clean_in_out_on_exit)
        signal.signal(signal.SIGINT, self.clean_in_out_on_exit)

    def clean_in_out_on_exit(self, *args):
        logging.info('clean_in_out_on_exit called')
        self.kill_dssat_subprocess()
        for io_file in self.in_out_location.glob("*"):
            try:
                io_file.unlink()
            except FileNotFoundError:
                pass
        self.remove_weather_file()
        try:
            self.in_out_location.rmdir()
        except FileNotFoundError:
            pass

    def kill_dssat_subprocess(self):
        try:
            self.dssat_proc.kill()
        except AttributeError:
            pass

    def remove_weather_file(self):
        try:
            self.in_files["WTH"].unlink()
        except FileNotFoundError:
            pass

    def build_fifos(self):
        """Builds the fifos used to interface with DSSAT."""
        self.build_in_files()
        self.build_out_fifos()

    def build_out_fifos(self):
        # DSSAT will automatically append to them
        self.out_fifos = {}
        for out_file in self.DSSAT_OUT_FILES:
            out_fifo = self.in_out_location / out_file
            os.mkfifo(out_fifo)
            self.out_fifos[out_file] = out_fifo

    def build_in_files(self):
        self.in_files = {}
        pid = os.getpid()
        exp_fifo = self.in_out_location / self.DSSAT_IN_FILES["EXP"]
        self.in_files["EXP"] = exp_fifo
        soil_fifo = self.in_out_location / self.DSSAT_IN_FILES["SOIL"]
        self.in_files["SOIL"] = soil_fifo
        wth_fifo = self.in_out_location / self.DSSAT_IN_FILES["WTH"].format(
            pid=str(pid)[-4:]
        )
        self.in_files["WTH"] = wth_fifo
        batch_fifo = self.in_out_location / self.DSSAT_IN_FILES["BATCH"].format(
            pid=str(pid)[-4:]
        )

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
            experiment = experiment._replace(
                weather_station_code=self.in_files["WTH"].stem
            )

        soil_file_string = None
        if experiment.soil_code is None:
            soil_file_string = str(experiment.soil_data)  # soil.Soil object
            experiment = experiment._replace(soil_code=experiment.soil_data.ROI_code)

        experiment_file_string = file_generator.generate_experiment_file_string(
            experiment
        )

        # Deploy write threads to write input files
        write_threads = self.deploy_write_threads(
            weather_file_string, experiment_file_string, soil_file_string
        )

        # Instance the Results class. It will spawn the read threads
        result = Results(self.out_fifos, experiment, self.in_out_location)

        # Join the write threads to ensure input files are written before
        # DSSAT is launched
        [write_thread.join() for write_thread in write_threads]

        # OK - finally - open a subprocess to run DSSAT from 'within'
        # the simulation's save directory, so that the files are saved there.
        self.start_dssat_subprocess(supress_stdout)

        # Tell the Results object to join read threads now we have run DSSAT
        result.read_outputs()

        return result

    def start_dssat_subprocess(self, supress_stdout):
        try:
            if supress_stdout:
                devnull = open(os.devnull, "w")
                self.dssat_proc = subprocess.Popen(
                    [self.dssat_exe, "A", self.in_files["EXP"].name],
                    cwd=self.in_out_location,
                    stdout=devnull,
                )
            else:
                self.dssat_proc = subprocess.Popen(
                    [self.dssat_exe, "A", self.in_files["EXP"].name],
                    cwd=self.in_out_location,
                )
        except FileNotFoundError:
            print("Simulation sub-dir not found. Exiting")
            exit()

    def deploy_write_threads(self, weather_string, experiment_string, soil_string):
        # NOTE: none of these can actually be fifos as DSSAT does double reads
        write_threads = []
        if soil_string is not None:
            write_threads.append(
                Thread(
                    target=self.write_string_to_file,
                    args=(soil_string, self.in_files["SOIL"]),
                    daemon=True,
                )
            )
        if weather_string is not None:
            write_threads.append(
                Thread(
                    target=self.write_string_to_file,
                    args=(weather_string, self.in_files["WTH"]),
                    daemon=True,
                )
            )
        write_threads.append(
            Thread(
                target=self.write_string_to_file,
                args=(experiment_string, self.in_files["EXP"]),
                daemon=True,
            )
        )
        [thread.start() for thread in write_threads]
        return write_threads

    def write_string_to_file(self, string, fifo):
        # Blocks until DSSAT goes to read from the fifo
        with open(fifo, "w") as f:
            f.write(string)

    def read_from_fifo(self, fifo):
        # Blocks until DSSAT writes to fifo
        with open(fifo, "r") as f:
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
            raise ValueError("Forecast start date must be full form YYYYDDD.")

        self.forecast_start = forecast_start
        self.num_years = num_years

    def _check_install(self, dssat_install):
        install = Path(dssat_install)
        exe_loc = install.glob("dscsm047")
        try:
            exe = next(exe_loc)
        except StopIteration:
            raise RuntimeError(
                "Could not find DSSAT exe in provided " f"directory at {install}"
            )
        return str(exe)


class AutomaticIrrigationManagement(NamedTuple):
    """Setting for automatic irrigation management."""

    irrigation_method_code: str = "IR001"  # See DSSAT DETAIL.CDE
    irrigation_management_depth: int = 30
    irrigation_threshold_lower: int = 50
    irrigation_threshold_upper: int = 100
    irrigation_stage_off: str = "GS000"
    irrigation_amount: int = 10
    irrigation_efficiency: float = 1


class Experiment(NamedTuple):
    crop: str
    model: str
    cultivar: str
    plant_date: datetime.date
    harvest_date: datetime.date
    simulation_start: datetime.date
    coordinates_latitude: float
    coordinates_longitude: float
    soil_data: soil.Soil = None
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
    experiment_ID: str = "DFLT"
    experiment_location_name: str = "DFLT"
    results_savelocation: str = None
    forecast_from_date: datetime.date = None
    num_forecast_years: int = None
    irrigation: str = "N"  # Can be R, N, A for reported, no, automatic
    irrigation_management: AutomaticIrrigationManagement = AutomaticIrrigationManagement()


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
    # NOTE: these are line skips for when using pipes to interface with DSSAT.
    #       DSSAT thinks that there is already a header in the existing file.
    file_layouts = {
        "maize": {
            "ERROR.OUT": None,
            "ET.OUT": 4,
            "Evaluate.OUT": 1,
            "INFO.OUT": None,
            "LUN.LST": None,
            "Mulch.OUT": 2,
            "N2O.OUT": 6,
            "OVERVIEW.OUT": None,
            "PlantGro.OUT": 4,
            "PlantN.OUT": 2,
            "RunList.OUT": None,
            "SoilNBalSum.OUT": 8,
            "SoilNiBal.OUT": None,
            "SoilNi.OUT": 5,
            "SoilNoBal.OUT": None,
            "SoilTemp.OUT": 6,
            "SoilWatBal.OUT": None,
            "SoilWat.OUT": 4,
            "Summary.OUT": None,
            "WARNING.OUT": None,
            "Weather.OUT": 2,
        }
    }

    def __init__(self, output_fifos, experiment, in_out_location):
        self.output_fifos = output_fifos
        self.experiment = experiment
        self.in_out_location = in_out_location
        self.crop = self.experiment.crop.lower()
        self.read_threads = self.start_read_threads()

    def start_read_threads(self):
        read_threads = {}
        for fifo_name in self.output_fifos:
            read_threads[fifo_name] = ThreadWithReturnValueAndException(
                target=self._load_table,
                args=(
                    self.output_fifos[fifo_name],
                    self.file_layouts[self.crop][fifo_name],
                ),
                daemon=True,
            )
            read_threads[fifo_name].start()
        return read_threads

    def read_outputs(self):
        results = self.get_results_from_read_threads(self.read_threads)
        # Set results tables as attributes of object
        for result in results:
            setattr(self, result.split(".")[0], results[result])
        # read Overview file
        self._set_overview(self.in_out_location / "OVERVIEW.OUT")

    def get_results_from_read_threads(self, read_threads):
        results = {}
        for fifo_name in sorted(read_threads):
            results[fifo_name] = read_threads[fifo_name].join_with_exception()
        return results

    def _load_table(self, fifo_loc, skiprows=0, index="DOY", numrows=None):

        try:
            with open(fifo_loc, "r") as fifo:
                r, w, e = select.select([fifo], [], [fifo], 3)  # timeout 3 seconds
                if not (r or w or e):
                    raise SimulationFailedError(f"Timeout on file {fifo_loc}.")
                out_string = fifo.read()
        except FileNotFoundError:
            return None

        # Skip must be after read so that DSSAT can write to fifo
        if skiprows is None:
            return None

        try:
            table = pd.read_csv(
                StringIO(out_string),
                sep="\s+",
                skiprows=skiprows,  # noqa
                nrows=numrows,
            )
        except pd.errors.EmptyDataError:
            logging.info(f"Result file {fifo_loc} empty.")
            raise SimulationFailedError(f"Result file {fifo_loc} empty.")

        if "DOY" in table.columns:
            DOY_leading_zeroes = table["DOY"].apply("{:0>3}".format)
            year_day = (table["@YEAR"].astype(str) + DOY_leading_zeroes).astype(int)
            table.index = year_day

        return table

    def _load_INFO(self, info_loc):
        # Load INFO.OUT file special case
        # TODO: lift other information from this file as needed
        # TODO: write splitter here to get fileshape independent
        # soil table extraction
        # Load soil information in
        # NOTE: consider splitting string on CLAY SILT SOIL keywords
        try:
            with open(info_loc, "r") as fifo:
                table_string = fifo.read().split("Soil ID")[1]
        except FileNotFoundError:
            return  # No info file generated
        # Remove file at the end otherwise DSSAT will stack results
        info_loc.unlink()
        table = pd.read_csv(
            StringIO(table_string), sep="\s+", skiprows=6, nrows=10  # noqa
        )
        table["Depth"] = [x[1] for x in table.index]
        table = table.drop(table.index[0])
        table.index = [int(x) for x in table.index.levels[0][:-1]]
        self.SoilInfo = table

    def _set_overview(self, overview_loc):
        try:
            with open(overview_loc, "rb") as f:
                overview = f.read().decode("unicode_escape")
        except FileNotFoundError:
            return  # No overview file generated
        self.overview = overview
        with open(overview_loc, "rb") as f:
            crop_info = f.readlines()[11].decode("unicode_escape")
        self.crop_info = crop_info

        overview_sections = overview.split("*")
        for x in overview_sections:
            if "SIMULATED CROP AND SOIL STATUS AT MAIN DEVELOPMENT STAGES" in x:
                # Need to format the table for easy pandas load
                def add_space(line):
                    return line[:12] + " " + line[12:]

                lines = x.split("\n")
                lines = map(add_space, lines)
                x = "\n".join(lines)
                x = StringIO(x)
                growth_stage_table = pd.read_csv(
                    x,
                    sep="\s\s+",  # noqa
                    skiprows=7,
                    skipinitialspace=True,
                    names=["Growth Stage", "GSTD_code"],
                    usecols=[2, 12],
                    engine="python",
                )
                growth_stage_table["Start"] = None
                growth_stage_table["End"] = None
                for index, row in growth_stage_table.iterrows():
                    try:
                        gstd = self.PlantGro[self.PlantGro["GSTD"] == row["GSTD_code"]]
                        start = gstd.index[0]
                        end = gstd.index[-1]
                    except IndexError:  # No row with this stage
                        continue
                    growth_stage_table.loc[index, "Start"] = start
                    growth_stage_table.loc[index, "End"] = end
                growth_stage_table.index = growth_stage_table["GSTD_code"]
                self.GrowthTable = growth_stage_table

        # Remove file at the end otherwise DSSAT will stack results
        overview_loc.unlink()

    def __str__(self):
        return self.overview


class ThreadWithReturnValueAndException(Thread):
    """Used to return value from a thread.
    See: https://stackoverflow.com/questions/6893968"""

    def __init__(
        self,
        group=None,
        target=None,
        name=None,
        daemon=None,
        args=(),
        kwargs={},
        Verbose=None,
    ):
        Thread.__init__(self, group, target, name, args, kwargs, daemon=daemon)
        self._return = None
        self.__status_queue = queue.Queue()

    def run_with_exception(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def join(self, timeout=None):
        Thread.join(self, timeout)
        return self._return

    def run(self):
        """This method should NOT be overriden."""
        try:
            self.run_with_exception()
        except BaseException:
            self.__status_queue.put(sys.exc_info())
        self.__status_queue.put(None)

    def wait_for_exc_info(self):
        return self.__status_queue.get()

    def join_with_exception(self):
        ex_info = self.wait_for_exc_info()
        if ex_info is None:
            return self.join()
        else:
            raise ex_info[1]
