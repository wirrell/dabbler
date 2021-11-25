"""
file_generator.py

Generates formatted DSSAT input files.
"""
import warnings
import pandas as pd
import numpy as np
from pathlib import Path
from .headers import get_header
from . import dabbler_errors
from . import templates

def gen_weather_name(ASD, county, year, gen_names):
    # Generate a weather file name
    if ASD == 'C':
        ASD_name = 'CC'
    else:
        ASD_name = ASD
    weather_name = (f'{ASD_name}{county[0].upper()}'
                    f'{county[1].upper()}{str(year)[2:]}01.WTH')
    if weather_name in gen_names:
        weather_name = (f'{ASD_name}{county[0].upper()}'
                        f'{county[2].upper()}{str(year)[2:]}01.WTH')

    return weather_name


def generate_experiment_file_string(experiment):
    """Generate the experiment file from the MZIXM template.MZX

    Parameters
    ----------
    experiment : dabbler.Experiment

    Returns
    -------
    str
        Formatted experiment file string
    """
    if experiment.weather_station_code is None:
        raise ValueError('Experiment weather station code not set.')
    if experiment.soil_code is None:
        raise ValueError('Experiment soil code not set.')

    # Read in the experiment template file
    template = templates.get_template_for_crop(experiment.crop) 

    terms = {'FLD_ID': experiment.experiment_ID[3:],
             'ID': experiment.experiment_ID,
             'MDL': experiment.model,
             'CULT': experiment.cultivar,
             'SDT': experiment.simulation_start.strftime('%y%j'),
             'SOIL_IDN': experiment.soil_code,
             'PLF': experiment.plant_date.strftime('%y%j'),
             'PLL': experiment.plant_date.strftime('%y%j')\
             if experiment.plant_end is None\
             else experiment.plant_end.strftime('%y%j'),
             'HVF': experiment.harvest_date.strftime('%y%j'),
             'HVL': experiment.harvest_date.strftime('%y%j')\
             if experiment.harvest_end is None\
             else experiment.harvest_end.strftime('%y%j'),
             'HDT': experiment.harvest_date.strftime('%y%j'),
             'WST': experiment.weather_station_code,
             'LOC': experiment.experiment_location_name}

    if experiment.forecast_from_date is not None:
        terms['FODAT'] = experiment.forecast_from_date  
        # must pad for formatting
        if experiment.num_forecast_years is None:
            raise ValueError('Must specify Experiment.num_forecast_years.')
        terms['NYR'] = '{0: >5}'.format(str(experiment.num_forecast_years))

    experiment_file_string = template.format(**terms)

    return experiment_file_string 


def generate_weather_file_string(experiment):
    """Generate a DSSAT weather file string from an experiment.

    Parameters
    ----------
    experiment : dabbler.Experiment
    header_data : dict
        with keys 'INSI', 'LAT', 'LONG', 'ELEV', 'TAV', 'AMP'
                       'REFHT', 'WNDHT', 'location'
    Returns
    -------
    str
        Absolute path to newly generated file.
    """

    if experiment.weather_data is None and experiment.weather_station_code is None:
        raise dabbler_errors.NoWeatherInformationError(
            "No weather information set. You must either set Experiment.weather_data"
            " or Experiment.weather_station_code."
        )
    
    # Check for any missing weather columns and fill them with NaN
    weather_columns = ['@DATE', 'SRAD',  'TMAX',
                       'TMIN', 'RAIN', 'DEWP', 'WIND',
                       'PAR', 'EVAP', 'RHUM']

    weather_data = weather_data.round(1)

    # Make wind data a whole int
    if 'WIND' in weather_data.columns:
        weather_data['WIND'] = weather_data['WIND'].astype(int)

    weather_data = fill_missing_columns_with_nan(weather_data, weather_columns)

    header_data = collate_weather_header_information(experiment)

    header_string = build_header('weather', header_data)

    dataframe_string = format_weather_dataframe_for_DSSAT(weather_data,
                                                          weather_columns)

    full_string = header_string + '\n' + dataframe_string

    return full_string


def format_weather_dataframe_for_DSSAT(dataframe, columns):
    # Set proper format justifications for columns 
    justify = ['left'] + ['right'] * 9
    # Have to split and strip to get formatting exactly right for DSSAT
    strings = dataframe.to_string(index=False,
                                  columns=columns,
                                  na_rep='   ',
                                  justify=justify).split('\n')
    strings = [x.strip() for x in strings]
    string = '\n'.join(strings)

    # replace -99.0 values that are a result of formatting with DSSAT
    # friendly -99 values
    string = string.replace('-99.0', '  -99')
    string = string.replace('-99.00', '   -99')

    return string


def fill_missing_columns_with_nan(dataframe, required_columns):
    for col in required_columns:
        if col not in dataframe.columns:
            dataframe[col] = np.nan
    return dataframe


def generate_weather(filename, savepath, weather_data, header_data):
    """Generate a DSSAT weather file.

    Parameters
    ----------
    filename : str
        e.g. 'UFGA8201.WTH'
    savepath : str
        directory to save generated file in
    weather_data : pandas.DataFrame
        Columns as per weather '@DATE', 'SRAD', 'TMAX', 'TMIN', 'RAIN', etc.
    header_data : dict
        with keys 'INSI', 'LAT', 'LONG', 'ELEV', 'TAV', 'AMP'
                       'REFHT', 'WNDHT', 'location'
    Returns
    -------
    str
        Absolute path to newly generated file.
    """
    
    path = Path(savepath, filename)

    # Check for any missing weather columns and fill them with NaN
    weather_columns = ['@DATE', 'SRAD',  'TMAX',
                       'TMIN', 'RAIN', 'DEWP', 'WIND',
                       'PAR', 'EVAP', 'RHUM']


    weather_data = weather_data.round(1)

    # Make wind data a whole int
    if 'WIND' in weather_data.columns:
        weather_data['WIND'] = weather_data['WIND'].astype(int)

    for col in weather_columns:
        if col not in weather_data.columns:
            weather_data[col] = np.nan

    # Set proper format justifications for solumns 
    justify = ['left'] + ['right'] * 9

    if path.exists():
        warnings.warn('Found existing weather file in specified location:'
                      f' {path}',
                       RuntimeWarning)


    with open(path, 'w') as f:
        # Write the header to file
        build_header(f, 'weather', header_data)

        # Have to split and strip to get formatting exactly right for DSSAT
        strings = weather_data.to_string(index=False,
                                       columns=weather_columns,
                                       na_rep='   ',
                                       justify=justify).split('\n')
        strings = [x.strip() for x in strings]
        string = '\n'.join(strings)

        # replace -99.0 values that are a result of formatting with DSSAT
        # friendly -99 values
        string = string.replace('-99.0', '  -99')
        string = string.replace('-99.00', '   -99')

        f.write(string)
        f.close()

    return str(path.absolute())


def collate_weather_header_information(experiment):
    header_data = {
        'INSI': '   ',
        'LAT': experiment.coordinates_latitude,
        'LONG': experiment.coordinates_longitude,
        'ELEV': -99,
        'TAV': -99,
        'AMP': -99,
        'REFHT': -99,
        'WNDHT': -99,
        'location': experiemnt.experiment_location_name
    }
    if experiment.elevation is not None:
        header_data['ELEV'] = experiment.elevation
    if experiment.average_soil_temperature is not None:
        header_data['TAV'] = experiment.average_soil_temperature
    if experiment.average_soil_temp_amplitude is not None:
        header_data['AMP'] = experiment.average_soil_temp_amplitude
    if experiment.weather_measurements_refernce_height is not None:
        header_data['REFHT'] = experiment.weather_measurements_refernce_height
    if experiment.wind_measurements_refernce_height is not None:
        header_data['WNDHT'] = experiment.wind_measurements_refernce_height

    return header_data






def build_header(filetype, data):
    """Builds the header and returns open file object.

    Parameters
    ----------
    filetype : str
        'weather' 'experiment'
    data : dict
        Data with releveant header data needed. Indexed by
        DSSAT header names e.g. 'ELEV' 'LAT' 'LONG'
        plus 'location' for location

    Returns
    -------
    _io.TextIOWrapper
        Open file wrapper with header written to it
    """
    header = get_header(filetype)

    # Format the header using the data dict
    header = header.format(**data)

    return header


def generate_batchfile_string(experiment, EXP_fifo):
    """Generate a DSSAT batch file.

    Parameters
    ----------
    experiment : dabbler.Experiment
    EXP_fifo : pathlib.Path for experiment fifo object

    Returns
    -------
    str
        Generated batchfile string

    Note
    ----
    Generates a very simple 2 line batch file. Used to run single
    experiments from a different directory as DSSAT does not like single
    experiment files being passed that are not in the same directory as the
    DSSAT file.
    """

    header_data = {'CROP': experiment.crop}

    # Format line string
    line = f'{str(EXP_fifo):<98}1      1      0      0      0'

    header_string = build_header('batch', header_data)

    batch_string = header_string + '\n' + line

    return batch_string


def generate_batchfile(experiment_filepath, savepath, crop_type, exp_number=''):
    """Generate a DSSAT batch file.

    Parameters
    ----------
    experiment_filepath : str
        Full path to experiment file eg '/home/test/UFGA8201.MZX'
    savepath : str
        directory to save generated file in
    crop_type : str
        eg 'MAIZE', for header of batch file
    exp_number : str, opt
        Identifier string if multiple batch files are being generated.
        If not specified, script may overwrite any default named batch
        files in the savepath location.

    Returns
    -------
    str
        Path to generated batch file

    Note
    ----
    Generates a very simple 2 line batch file. Used to run single
    experiments from a different directory as DSSAT does not like single
    experiment files being passed that are not in the same directory as the
    DSSAT file.
    """

    if exp_number == '':
        exp_number = 'DSSAT'
    path = Path(savepath, f'{exp_number}.v47')

    header_data = {'CROP': crop_type}

    # Format line string
    line = f'{experiment_filepath:<98}1      1      0      0      0'

    with open(path, 'w') as f:
        # Write the header to file
        build_header(f, 'batch', header_data)

        f.write(line)

        f.close()

    return str(path.name)


if __name__ == '__main__':

    # Read in from a DSSAT weather file for testing
    wth_file = 'UFGA8201.WTH'

    wth = pd.read_csv(wth_file, skiprows=4, sep='\s+')

    header_data = {'INSI' : 'UFGA',
                   'LAT' : 29.630,
                   'LONG' : -82.370,
                   'ELEV' : 10,
                   'TAV' : 20.9,
                   'AMP' : 13.0,
                   'REFHT' : 2.00,
                   'WNDHT' : 3.00,
                   'location': 'Test'}


    generate_weather('test.WTH', '', wth, header_data)
    generate_batchfile('/home/george/Documents/code/dabbler/EXP_template.MZX', 'run', 'MAIZE',
                       '1234')

