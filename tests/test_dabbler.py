import os
import sys
import pipes
import json
import signal
import time
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dabbler import DSSAT, Experiment, Results
import dabbler.soil
import difflib
from datetime import date
from shapely.geometry import Polygon
from pathlib import Path
from threading import Thread
import pytest

dssat_bin = '/home/george/DSSAT/build/bin'
dssat_weather = '/home/george/DSSAT/build/Weather'
dssat_soil = '/home/george/DSSAT/build/Soil'

@pytest.fixture(scope='module')
def dssat_instance():
    return DSSAT(dssat_bin, dssat_weather, dssat_soil)


@pytest.fixture(scope='module')
def experiment():
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
    def test_field(self):
        with open('test_data/field.geojson', 'r') as field_file:
            field_json = json.load(field_file)
        field = Polygon(field_json['features'][0]['geometry']['coordinates'][0])
        return field

    @pytest.fixture()
    def experiment_with_custom_soil(self, test_field):
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
            soil_data=dabbler.soil.SoilGenerator().build_soils({'TEST000001': test_field})[0]
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

    def test_out_fifos_have_been_created(self, dssat_instance):
        fifos_created = [x.name for x in
                         dssat_instance.in_out_location.glob('*.OUT')]
        for fifo in self.DSSAT_OUT_FILES:
            assert (fifo in fifos_created)

    def test_EXP_file_generated_correctly(self, EXP_file, reference_EXP_file):
        assert EXP_file == reference_EXP_file

    def test_deploy_write_thread_for_weather(self, dssat_instance, experiment):
        dummy_weather_string = 'DUMMY WEATHER'

        test_thread = Thread(target=dssat_instance.deploy_write_threads,
                      args=(dummy_weather_string, '', None))
        test_thread.start()

        # Give dabbler time to deploy files
        time.sleep(0.1)

        with open(dssat_instance.in_fifos['WTH'], 'r') as wth:
            output = wth.read()
        
        assert dummy_weather_string == output

    def test_deploy_write_thread_for_soil(self, dssat_instance, experiment):
        dummy_soil_string = 'DUMMY SOIL'

        test_thread = Thread(target=dssat_instance.deploy_write_threads,
                      args=(None, '', dummy_soil_string))
        test_thread.start()

        # Give dabbler time to deploy files
        time.sleep(0.1)

        with open(dssat_instance.in_fifos['SOIL'], 'r') as soil:
            output = soil.read()

        # Remove soil file so other DSSAT runs don't look inside it for soil info
        dssat_instance.in_fifos['SOIL'].unlink()
        
        assert dummy_soil_string == output

    def test_run_returns_results(self, dssat_instance, experiment):
        assert isinstance(dssat_instance.run(experiment, supress_stdout=False), Results)

    def test_run_with_experiment_with_custom_soil_data(self, dssat_instance,
                                                       experiment_with_custom_soil):
        results = dssat_instance.run(experiment_with_custom_soil)

        # Remove soil file so other DSSAT runs don't look inside it for soil info
        dssat_instance.in_fifos['SOIL'].unlink()

        assert len(results.PlantGro) > 10



class TestResults:

    @pytest.fixture(scope='module')
    def empty_results_instance(self, dssat_instance):
        return Results(dssat_instance.out_fifos, 'maize')

    def test_all_read_threads_exit(self, dssat_instance, experiment):
        results = dssat_instance.run(experiment)
        for read_thread in results.read_threads:
            assert not results.read_threads[read_thread].is_alive()


    # def test_start_read_threads(self, empty_results_instance):
    # NOTE: works with DSSAT but hard to mimic DSSAT writing to pipe behaviour
    #       with python so commented out for now.
    #
    #        read_threads = empty_results_instance.start_read_threads()
    #        for fifo in sorted(empty_results_instance.output_fifos):
    #            fifo_loc = empty_results_instance.output_fifos[fifo]
    #            with Timeout(seconds=1):
    #                t = pipes.Template()
    #                f = t.open(fifo_loc, 'w')
    #                with open(f'test_data/{fifo}', 'r') as example:
    #                    # Write test string to check pipe is open. Will block if not.
    #                    example_str = example.read()
    #                    f.write(example_str)
    #
    #        for read_thread_name, read_thread in read_threads.items():
    #            print('-----------')
    #            print(read_thread_name)
    #            print(read_thread.is_alive())
    #            print('-----------')
    #
    #        assert False
    #
    #        assert True


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







