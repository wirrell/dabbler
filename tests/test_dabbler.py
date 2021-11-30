import os
import sys
import signal
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dabbler import DSSAT, Experiment, Results
from datetime import date
from pathlib import Path
from threading import Thread
import pytest

dssat_bin = '/home/george/DSSAT/build/bin'
dssat_weather = '/home/george/DSSAT/build/Weather'
dssat_soil = '/home/george/DSSAT/build/Soil'

@pytest.fixture(scope='module')
def dssat_instance():
    return DSSAT(dssat_bin, dssat_weather, dssat_soil)

class TestDabbler:

    DSSAT_OUT_FILES = [
        # 'ERROR.OUT',
        'ET.OUT',
        'Evaluate.OUT',
        # 'INFO.OUT',
        # 'LUN.LST',
        'Mulch.OUT',
        # 'N2O.OUT', NOTE: not currently used in our applications
        # 'OVERVIEW.OUT', NOTE: skip overview for now as it requires bytes read
        'PlantGro.OUT',
        'PlantN.OUT',
        'RunList.OUT',
        # 'SoilNBalSum.OUT',
        # 'SoilNiBal.OUT',
        # 'SoilNi.OUT',
        # 'SoilNoBal.OUT',
        'SoilTemp.OUT',
        'SoilWatBal.OUT',
        'SoilWat.OUT',
        'Summary.OUT',
        # 'WARNING.OUT',
        'Weather.OUT'
    ]

    @pytest.fixture()
    def experiment(self):
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

        return experiment

    @pytest.fixture()
    def EXP_file(self, dssat_instance, experiment):
        result = dssat_instance.run(experiment)
        EXP_file = next(dssat_instance.in_out_location.glob('*.EXP'))
        with open(EXP_file, 'r') as f:
            return f.read()

    @pytest.fixture()
    def reference_EXP_file(self):
        with open('test_data/PIPE0001.EXP', 'r') as f:
            return f.read()

    def test_run_returns_results(self, dssat_instance, experiment):
        assert isinstance(dssat_instance.run(experiment), Results)

    def test_out_fifos_have_been_created(self, dssat_instance):
        fifos_created = [x.name for x in
                         dssat_instance.in_out_location.glob('*.OUT')]
        for fifo in self.DSSAT_OUT_FILES:
            assert (fifo in fifos_created)

    def test_weather_fifo_has_been_created(self, dssat_instance):
        wth_file =  Path(dssat_weather).glob('PIPE*.WTH')
        assert len(list(wth_file)) == 1

    def test_EXP_file_generated_correctly(self, EXP_file, reference_EXP_file):
        assert EXP_file == reference_EXP_file

    def test_deploy_write_thread_for_weather(self, dssat_instance, experiment):
        dummy_weather_string = 'DUMMY WEATHER'

        test_thread = Thread(target=dssat_instance.deploy_write_threads,
                      args=(dummy_weather_string, ''))
        test_thread.start()

        with open(dssat_instance.in_fifos['WTH'], 'r') as wth:
            output = wth.read()
        
        assert dummy_weather_string == output


class TestResults:


    @pytest.fixture(scope='module')
    def empty_results_instance(self, dssat_instance):
        return Results(dssat_instance.out_fifos, 'maize')

    def test_start_read_threads(self, empty_results_instance):

        raise NotImplementedError('FIX issue with threads not dissappearing after test.')

        read_threads = empty_results_instance.start_read_threads()
        for fifo, fifo_loc in empty_results_instance.output_fifos.items():
            with Timeout(seconds=1):
                with open(fifo_loc, 'w') as f:
                    # Write empty string to check pipe is open. Will block if not.
                    f.write('CAUSES ERROR')

        assert True


class Timeout:
    # https://stackoverflow.com/questions/2281850
    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message
    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)
    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)
    def __exit__(self, type, value, traceback):
        signal.alarm(0)







