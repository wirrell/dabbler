"""
file_generator.py

Generates formatted DSSAT input files.
"""
import warnings
import pandas as pd
import numpy as np
from pathlib import Path
from .headers import get_header

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


def generate_experiment(ID, model, plant_start, plant_end, harvest_date, harvest_start,
                        harvest_end, simulation_start, cultivar, soil_id, loc,
                        save_loc, save_name, template, forecast_start=False,
                        num_years=False):
    """Generate the experiment file from the MZIXM template.MZX

    Parameters
    ----------
    ID : str
        simulation ID
    plant_start : int
        YYDOY
    plant_end : int
        YYDOY
    harvest_date : int
        YYDOY
    harvest_start : int
        YYDOY - now defunct but left in incase we need it later
    harvest_end : int
        YYDOY - now defunct but left in incase we need it later
    simulation_start : int
        YYDOY
    cultivar : str
        e.g. 'PC0004'
    soil_id : str
        e.g. 'IB00000007'
    loc : str
        Location, e.g. 'Iowa ASD: NW, County: Emmet'
    save_loc : str
        Where to save the generated experiment file
    save_name : str
        File savename
    template : str
        Where the template is located
    forecast_start : int, opt
        If forecast run, when to start forecast. Must be full form (YYYYDDD).
    num_years : int
        If forecast run, numbers of years to pull from historical weather for the run.

    Returns
    -------
    str
        Path to generated experiment file.
    """

    # Read in the experiment template file
    with open(template, 'r') as f:
        template = f.read()

    terms = {'FLD_ID': ID[3:],
             'ID': ID,
             'MDL': model,
             'CULT': cultivar,
             'SDT': simulation_start,
             'SOIL_IDN': soil_id,
             'PLF': plant_start,
             'PLL': plant_end,
             'HVF': harvest_start,
             'HVL': harvest_end,
             'HDT': harvest_date,
             'WST': save_name[:4],
             'LOC': loc}

    if forecast_start:
        terms['FODAT'] = forecast_start
        terms['NYR'] = '{0: >5}'.format(str(num_years))  # must pad for formatting

    template = template.format(**terms)
    save_loc = Path(save_loc)
    try:
        save_loc.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        pass
    with open(save_loc.joinpath(save_name), 'w') as f:
        f.write(template)

    return save_name


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


def build_header(f, filetype, data):
    """Builds the header and returns open file object.

    Parameters
    ----------
    f : io.TextIOWrapper
        Open file wrapper
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

    f.write(header)


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

