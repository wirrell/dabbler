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
from datetime import date, datetime


def generate_weather(coordinates, year, loc_name, filename, savepath, source="DayMet"):
    """Generate a DSSAT weather file from DayMet weather data.

    Parameters
    ----------
    coordinates : list
        [lon, lat] WGS84
    loc_name : str
        Location name
    year : int
        Year to generate file for, e.g. 2019
    filename : str
        e.g. 'UFGA1901.WTH'
    savepath : str
        directory to save generated file in
    source : str
        'DayMet' or 'NASA-POWER' available.

    Returns
    -------
    str
        Absolute path to newly generated file.
    """

    # Get the weather data
    if source == "DayMet":
        weather_data, elev, t_avg = get_daymet_data(
            coordinates[0], coordinates[1], year
        )
    elif source == "NASA-POWER":
        weather_data, elev, t_avg = get_nasa_power_data(
            coordinates[0], coordinates[1], year
        )
    else:
        raise ValueError("'DayMet' or 'NASA-POWER' available.")

    # Form the header information
    header_data = {
        "INSI": loc_name.split()[0],
        "LAT": coordinates[0],
        "LONG": coordinates[1],
        "ELEV": elev,
        "TAV": t_avg,
        "AMP": -99,
        "REFHT": -99,
        "WNDHT": -99,
        "location": loc_name,
    }

    path = Path(savepath, filename)

    # Check for any missing weather columns and fill them with NaN
    weather_columns = [
        "@DATE",
        "SRAD",
        "TMAX",
        "TMIN",
        "RAIN",
        "DEWP",
        "WIND",
        "PAR",
        "EVAP",
        "RHUM",
    ]

    weather_data = weather_data.round(1)

    for col in weather_columns:
        if col not in weather_data.columns:
            weather_data[col] = np.nan

    # Set proper format justifications for columns
    justify = ["left"] + ["right"] * 9

    with open(path, "w") as f:
        # Write the header to file
        _build_header(f, "weather", header_data)

        # Have to split and strip to get formatting exactly right for DSSAT
        strings = weather_data.to_string(
            index=False, columns=weather_columns, na_rep="   ", justify=justify
        ).split("\n")
        strings = [x.strip() for x in strings]
        string = "\n".join(strings)

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


def get_daymet_data(lon, lat, year):
    """Pull the DayMet data for the field from the ORNL ReST server.

    https://daymet.ornl.gov/web_services#single

    Parameters
    ----------
    field_df : pandas.DataFrame

    Returns
    -------
    field_df : pandas.DataFrame
    """

    query = (
        "https://daymet.ornl.gov/single-pixel/api/data?"
        "lat={lat}&lon={lon}&years={year}"
    )

    query = query.format(lat=lat, lon=lon, year=year)

    r = requests.get(query)

    while r.status_code != 200:
        print(f"Request to DayMet is currently at code: {r.status_code}")
        print("Waiting 30 seconds before next request.")
        time.sleep(30)

    start_of_year = int(str(year)[2:] + "001")
    end_of_year = int(str(year)[2:] + "365")

    index = range(start_of_year, end_of_year + 1)
    field_df = pd.DataFrame(index=index)

    weather = pd.read_csv(io.StringIO(r.text), skiprows=7)
    weather.index = field_df.index

    # Do SRAD calc to get from W/m2 to MJ / (m2*day) which DSSAT needs
    watts = weather["srad (W/m^2)"]
    MJ_hour = watts * 0.0036
    MJ_day = MJ_hour * (weather["dayl (s)"] / (60 ** 2))

    field_df["@DATE"] = weather.index
    field_df["RAIN"] = weather["prcp (mm/day)"]
    field_df["SRAD"] = MJ_day
    field_df["TMAX"] = weather["tmax (deg c)"]
    field_df["TMIN"] = weather["tmin (deg c)"]

    # Read elevation info off here
    elev_line = r.text.split("\n")[3]
    elev = int(elev_line.split()[1])
    t_avg = np.mean((field_df["TMAX"] + field_df["TMIN"]) / 2)

    return field_df, elev, t_avg


def get_nasa_power_data(lon, lat, year):

    start_date, end_date = generate_start_and_end_date(year)

    variables = [
        "T2M_MAX",
        "T2M_MIN",
        "T2MDEW",
        "PRECTOTCORR",
        "WS2M",
        "RH2M",
        "ALLSKY_SFC_SW_DWN",
    ]
    weather, elevation = get_POWER_singlepoint(
        (lon, lat), start_date, end_date, variables
    )
    # convert to DSSAT weather
    dssat_weather = pd.DataFrame()
    dssat_weather["@DATE"] = [
        str(x)[2:] + f"{y:03}" for x, y in zip(weather["YEAR"], weather["DOY"])
    ]
    dssat_weather["SRAD"] = weather["ALLSKY_SFC_SW_DWN"]
    dssat_weather["TMAX"] = weather["T2M_MAX"]
    dssat_weather["TMIN"] = weather["T2M_MIN"]
    dssat_weather["DEWP"] = weather["T2MDEW"]
    # Convert m/s to km/d for NASA POWER to DSSAT
    dssat_weather["WIND"] = (weather["WS2M"] * (86400 / 1000)).astype(int)
    dssat_weather["RAIN"] = weather["PRECTOTCORR"]
    dssat_weather["RHUM"] = weather["RH2M"]
    # Mask any no-value points
    dssat_weather = dssat_weather.replace(-999, -99)
    dssat_weather = dssat_weather.replace(-86313, -99)  # no point wind

    dssat_weather.index = pd.DatetimeIndex(
        [datetime.strptime(x, "%y%j") for x in dssat_weather["@DATE"]]
    )
    t_avg = np.mean((dssat_weather["TMAX"] + dssat_weather["TMIN"]) / 2)

    return dssat_weather, elevation, t_avg


def generate_start_and_end_date(year):
    start_date = date(year, 1, 1)
    if year == datetime.now().year:
        end_date = datetime.now().date()
    else:
        end_date = datetime(year, 12, 31)

    return start_date, end_date


def get_POWER_singlepoint(coordinates, start_date, end_date, parameters):
    """
    Retrieve daily values for a single point from NASA POWER.

    See: https://power.larc.nasa.gov/docs/services/api/v1/temporal/daily/

    Parameters
    ----------
    coordinates : tuple
        (longitude, latitude) in WGS84
    start_date : datetime.date
    end_date : datetime.date
    parameters : list of str
        Parameters to request. 

    Returns
    -------
    pd.DataFrame
        with column names of the passed parameters
    float
        Location elevation in meters

    Notes
    ---
    Only 20 parameters can be requested at a time.
    All values are taken at 2m reference point.
    """
    if len(parameters) > 20:
        raise RuntimeError("NASA POWER allows only 20 parameters at a time.")

    power_url = "https://power.larc.nasa.gov/api/temporal/daily/point?"

    payload = {
        "parameters": ",".join(parameters),
        "community": "AG",
        "latitude": coordinates[1],
        "longitude": coordinates[0],
        "start": start_date.strftime("%Y%m%d"),
        "end": end_date.strftime("%Y%m%d"),
        "format": "CSV",
    }

    r = requests.get(power_url, params=payload)

    csv_io = io.BytesIO(r.content)
    # skip lines that are header + parameter details
    try:
        data = pd.read_csv(csv_io, skiprows=8 + len(parameters))
    except Exception as e:
        print(r.content)
        raise (e)

    # get elevation average from header
    header = str(r.content).split("-END HEADER-")[0]
    elev = float(header.split("=")[1].split("meters")[0].strip())

    return data, elev
