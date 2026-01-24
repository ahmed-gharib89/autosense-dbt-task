#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Helper module for data processing utilities.

Author: Ahmed Gharib
Created on: 2025-01-19
"""
import re
import pandas as pd
import reverse_geocoder as rg


# Helper function to remove timezone from datetime columns
def remove_timezone(df):
    """
    Remove timezone information from all datetime columns in the DataFrame.
    Parameters:
        df (pd.DataFrame): Input DataFrame with potential timezone-aware datetime columns.
    Returns:
        pd.DataFrame: DataFrame with timezone information removed from datetime columns.
    """
    df_copy = df.copy()
    for col in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].dt.tz_localize(None)
    return df_copy


def check_invalid_location_in_ch(df, lat_col="latitude", lon_col="longitude"):
    """
    Check for invalid geographical locations in the DataFrame within Switzerland.
    Parameters:
        df (pd.DataFrame): Input DataFrame with latitude and longitude columns.
        lat_col (str): Name of the latitude column.
        lon_col (str): Name of the longitude column.
    Returns:
        pd.Series: Boolean Series indicating valid locations.
    """
    valid_lat = df[lat_col].between(45.817995, 47.808455)
    valid_lon = df[lon_col].between(5.955911, 10.492294)
    return ~(valid_lat & valid_lon)

def get_city_from_location(df, lat_col="latitude", lon_col="longitude"):
    """
    Get city names based on latitude and longitude.
    Parameters:
        df (pd.DataFrame): Input DataFrame with latitude and longitude columns.
        lat_col (str): Name of the latitude column.
        lon_col (str): Name of the longitude column.
    Returns:
        pd.Series: Series containing city names.
    """
    coordinates = list(zip(df[lat_col], df[lon_col]))
    results = rg.search(coordinates)
    cities = [result['name'] for result in results]
    return pd.Series(cities)


def check_valid_email(email):
    """
    Check if the provided email address is valid.
    Parameters:
        email (str): Email address to validate.
    Returns:
        bool: True if the email is valid, False otherwise.
    """
    email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return re.match(email_regex, email) is not None


def flag_outliers(series):
    """
    Flag outliers in a pandas Series using the IQR method.
    Parameters:
        series (pd.Series): Input pandas Series.
    Returns:
        pd.Series: Boolean Series indicating outliers.
    """
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return (series < lower_bound) | (series > upper_bound)
