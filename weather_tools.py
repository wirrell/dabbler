"""
Format weather data ready for dabbler.
"""
import pandas as pd


def format_Iowa_mesonet(weather_file, station, year):
    """
    Format Iowa mesonet data.

    Parameteres
    -----------
    weather_file : str
        Path to mesonet .csv
    station : str
        station_name field from mesonet e.g. 'ESTHERVILLE-2-N'
    year : int
        year to extract

    Returns
    -------
    pandas.Dataframe
        Formatted with columns as per weather '@DATE', 'SRAD', 'TMAX',
        'TMIN', 'RAIN', etc.
    """

    mesonet_data = pd.read_csv(weather_file, skiprows=4)

    station_data = mesonet_data[mesonet_data['station_name'] == station].copy()
    station_data['date'] = pd.to_datetime(station_data['day'])
    station_data.index = station_data['date']
    station_data = station_data[f'1-{year}':f'12-{year}']


    formatted_data = pd.DataFrame()
    formatted_data['@DATE'] = station_data['date'].dt.strftime('%y%j')
    formatted_data['SRAD'] = station_data['narr_srad']
    formatted_data['TMAX'] = station_data['highc']
    formatted_data['TMIN'] = station_data['lowc']
    formatted_data['RAIN'] = station_data['precipmm']

    if formatted_data.isin(['M']).any().any():
        raise RuntimeError('Missing values ("M") in weather data.')

    header_data = {'LAT': station_data['lat'][0],
                   'LONG': station_data['lon'][0],
                   'INSI': station_data['station'][0],
                   'location': station,
                   'ELEV': -99,
                   'TAV': -99,
                   'AMP': -99,
                   'REFHT': -99,
                   'WNDHT': -99,
                  }
    for col in formatted_data:
        if 'DATE' in col:
            continue
        formatted_data[col] = formatted_data[col].astype(float)

    return formatted_data, header_data
