"""
Generate DSSAT weather files for anywhere in the CONUS.

Weather data taken from DayMet - https://daymet.ornl.gov/

(DayMet date range: 1980 to 2019)

Notes:
    Currently only does full year files, i.e. Jan 1st to Dec 31st.

Author: G. Worrall
Date: February 8th 2021
"""
import time
import io
import numpy as np
import pandas as pd
import requests
from .headers import get_header
from pathlib import Path

def generate_weather(coordinates, year, loc_name, filename, savepath):
    """Generate a DSSAT weather file from DayMet weather data.

    Parameters
    ----------
    coordinates : list
        [latitude, longitude] WGS84
    loc_name : str
        Location name
    year : int
        Year to generate file for, e.g. 2019
    filename : str
        e.g. 'UFGA1901.WTH'
    savepath : str
        directory to save generated file in

    Returns
    -------
    str
        Absolute path to newly generated file.
    """

    # Get the weather data
    weather_data, elev, t_avg = _get_daymet_data(coordinates[0],
                                                 coordinates[1],
                                                 year)

    # Form the header information
    header_data = {'INSI': loc_name.split()[0],
                   'LAT': coordinates[0],
                   'LONG': coordinates[1],
                   'ELEV': elev,
                   'TAV': t_avg,
                   'AMP': -99,
                   'REFHT': -99,
                   'WNDHT': -99,
                   'location': loc_name}

    path = Path(savepath, filename)

    # Check for any missing weather columns and fill them with NaN
    weather_columns = ['@DATE', 'SRAD',  'TMAX',
                       'TMIN', 'RAIN', 'DEWP', 'WIND',
                       'PAR', 'EVAP', 'RHUM']


    weather_data = weather_data.round(1)

    for col in weather_columns:
        if col not in weather_data.columns:
            weather_data[col] = np.nan

    # Set proper format justifications for columns 
    justify = ['left'] + ['right'] * 9


    with open(path, 'w') as f:
        # Write the header to file
        _build_header(f, 'weather', header_data)

        # Have to split and strip to get formatting exactly right for DSSAT
        strings = weather_data.to_string(index=False,
                                       columns=weather_columns,
                                       na_rep='   ',
                                       justify=justify).split('\n')
        strings = [x.strip() for x in strings]
        string = '\n'.join(strings)

        f.write(string)
        f.close()

    return str(path.absolute())


def _build_header(f, filetype, data):
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


def _get_daymet_data(lat, lon, year):
    """Pull the DayMet data for the field from the ORNL ReST server.

    https://daymet.ornl.gov/web_services#single

    Parameters
    ----------
    field_df : pandas.DataFrame

    Returns
    -------
    field_df : pandas.DataFrame
    """

    query = ('https://daymet.ornl.gov/single-pixel/api/data?'
              'lat={lat}&lon={lon}&years={year}')

    query = query.format(lat=lat, lon=lon, year=year)

    r = requests.get(query)

    while r.status_code != 200:
        print(f'Request to DayMet is currently at code: {r.status_code}')
        print('Waiting 30 seconds before next request.')
        time.sleep(30)

    start_of_year = int(str(year)[2:] + '001')
    end_of_year = int(str(year)[2:] + '365')

    index = range(start_of_year, end_of_year+1)
    field_df = pd.DataFrame(index=index)

    weather = pd.read_csv(io.StringIO(r.text), skiprows=7)
    weather.index = field_df.index

    # Do SRAD calc to get from W/m2 to MJ / (m2*day) which DSSAT needs
    watts = weather['srad (W/m^2)']
    MJ_hour = watts * 0.0036
    MJ_day = MJ_hour * (weather['dayl (s)'] / (60 ** 2))


    field_df['@DATE'] = weather.index
    field_df['RAIN'] = weather['prcp (mm/day)']
    field_df['SRAD'] = MJ_day
    field_df['TMAX'] = weather['tmax (deg c)']
    field_df['TMIN'] = weather['tmin (deg c)']

    # Read elevation info off here
    elev_line = r.text.split('\n')[3]
    elev = int(elev_line.split()[1])
    t_avg = np.mean((field_df['TMAX'] + field_df['TMIN']) / 2)

    return field_df, elev, t_avg


if __name__ == '__main__':
    test_loc = [42.98012257050593, -95.18055134835838]
    test_name = 'TEST1501.WTH'
    test_year = 2015
    save_path = '.'
    location = 'Test County, TE'
    new_file = generate_weather(test_loc, test_year, location, test_name,
                                save_path)
