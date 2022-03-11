"""
Pedotransfer Functions for estimating soil hydraulic properties.

Methodology taken from: 
    https://doi.org/10.2136/sssaj2005.0117
    https://doi.org/10.1016/j.envsoft.2019.05.012
"""
import numpy as np
import pandas as pd


def saturated_upper_limit(sand_w, clay_w, soc_w):
    """
    Calculate the saturated water content limit of a soil from sand, clay, and soil organic
    carbon by percentage weight.

    Saturated water content is the water content of the soil at 0 kPa.

    Soil organic carbon is converted to soil organic matter % w using a factor
    of 2 conversion. See:
        https://www.sciencedirect.com/science/article/pii/S0016706110000388

    Parameters
    ----------
    sand_w : float
        sand % weight of soil
    clay_w : float
        clay % weight of soil
    soc_w : float
        soil organic carbon % weight of soil

    Returns
    -------
    float
        soil water content at 0kPa
    """
    # Convert soil organic carbon to soil organic matter
    om_w = soc_w * 2

    # First we calculate theta_s_33 which is the difference between theta_s
    # - theta_33
    theta_s_33_t = (
        0.278 * sand_w
        + 0.034 * clay_w
        + 0.022 * om_w
        - 0.018 * (sand_w * om_w)
        - 0.027 * (clay_w * om_w)
        - 0.584 * (sand_w * clay_w)
        + 0.078
    )
    theta_s_33 = theta_s_33_t + (0.6360 * theta_s_33_t - 0.107)

    theta_s = (
        drained_upper_limit(sand_w, clay_w, soc_w) + theta_s_33 - 0.097 * sand_w + 0.043
    )

    return theta_s


def drained_upper_limit(sand_w, clay_w, soc_w):
    """
    Calculate the drained upper limit of a soil from sand, clay, and soil organic
    carbon by percentage weight.

    Drained upper limit is the water content of the soil at -33 kPa.

    Soil organic carbon is converted to soil organic matter % w using a factor
    of 2 conversion. See:
        https://www.sciencedirect.com/science/article/pii/S0016706110000388

    Parameters
    ----------
    sand_w : float
        sand % weight of soil
    clay_w : float
        clay % weight of soil
    soc_w : float
        soil organic carbon % weight of soil

    Returns
    -------
    float
        soil water content at -33kPa
    """
    # Convert soil organic carbon to soil organic matter
    om_w = soc_w * 2

    theta_33t = (
        -0.251 * sand_w
        + 0.195 * clay_w
        + 0.011 * om_w
        + 0.006 * (sand_w * om_w)
        - 0.027 * (clay_w * om_w)
        + 0.452 * (sand_w * clay_w)
        + 0.299
    )

    theta_33 = theta_33t + (1.283 * (theta_33t ** 2) - (0.374 * theta_33t) - 0.015)

    return theta_33


def wilting_point(sand_w, clay_w, soc_w):
    """
    Calculate the wilting point of a soil from sand, clay, and soil organic
    carbon by percentage weight.

    Wilting point is the water content of the soil at -1500 kPa.

    Soil organic carbon is converted to soil organic matter % w using a factor
    of 2 conversion. See:
        https://www.sciencedirect.com/science/article/pii/S0016706110000388

    Parameters
    ----------
    sand_w : float
        sand % weight of soil
    clay_w : float
        clay % weight of soil
    soc_w : float
        soil organic carbon % weight of soil

    Returns
    -------
    float
        soil water content at -1500kPa
    """
    # Convert soil organic carbon to soil organic matter
    om_w = soc_w * 2

    theta_1500t = (
        -0.024 * sand_w
        + 0.487 * clay_w
        + 0.006 * om_w
        + 0.005 * (sand_w * om_w)
        - 0.013 * (clay_w * om_w)
        + 0.068 * (sand_w * clay_w)
        + 0.031
    )
    theta_1500 = theta_1500t + (0.14 * theta_1500t - 0.02)

    return theta_1500


def slope_of_log_tension_moisture_curve(theta_33, theta_1500):
    """
    Calculate lambda, the slop of the logarithmic tension moisture curve
    """
    # Force types so a passed pandas series can be used with numpy.log
    if isinstance(theta_33, pd.Series):
        theta_33 = theta_33.astype(float)
    if isinstance(theta_1500, pd.Series):
        theta_1500 = theta_1500.astype(float)

    lam = 1 / (((np.log(1500) - np.log(33)) / (np.log(theta_33) - np.log(theta_1500))))

    return lam


def saturated_hydraulic_conductivity(sand_w, clay_w, soc_w):
    """
    Calculate the soil hydraulic conductivity of a soil from sand, clay,
    and soil organic carbon by percentage weight.

    Wilting point is the water content of the soil at -1500 kPa.

    Soil organic carbon is converted to soil organic matter % w using a factor
    of 2 conversion. See:
        https://www.sciencedirect.com/science/article/pii/S0016706110000388

    Parameters
    ----------
    sand_w : float
        sand % weight of soil
    clay_w : float
        clay % weight of soil
    soc_w : float
        soil organic carbon % weight of soil

    Returns
    -------
    float
        soil hydraulic conductivity in mm h^-1
    """
    theta_s = saturated_upper_limit(sand_w, clay_w, soc_w)
    theta_33 = drained_upper_limit(sand_w, clay_w, soc_w)
    theta_1500 = wilting_point(sand_w, clay_w, soc_w)
    lam = slope_of_log_tension_moisture_curve(theta_33, theta_1500)

    K_s = 1930 * (theta_s - theta_33) ** (3 - lam)

    return K_s
