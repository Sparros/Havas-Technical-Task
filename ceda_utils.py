# encoding: utf-8
"""
CEDA Archive Data Access Utilities

This module provides utility functions for accessing weather data from the CEDA Archive.

CEDA now uses OAuth2 access tokens instead of certificates.
Generate a token at: https://services.ceda.ac.uk/account/token/

Original adapted from: https://github.com/cedadev/opendap-python-example

Before running the script, please install pandas:

pip install pandas

And set the following environment variables:

- CEDA_USERNAME: Your CEDA username (to allow for refreshing access tokens if needed)
- CEDA_PASSWORD: Your CEDA password (to allow for refreshing access tokens if needed)

OR
- CEDA_ACCESS_TOKEN: Your CEDA OAuth2 access token (if you do not want the script to automatically refresh tokens)
"""

import io
import json
import os
import re
import requests
import pandas as pd
from base64 import b64encode


def refresh_access_token(username=None, password=None):
    """
    Refresh CEDA access token using username and password.

    This function calls the CEDA API to generate a new access token.

    Args:
        username (str, optional): CEDA username. If not provided, will use CEDA_USERNAME env var.
        password (str, optional): CEDA password. If not provided, will use CEDA_PASSWORD env var.

    Returns:
        str: New access token

    Raises:
        KeyError: If credentials are not provided and environment variables are not set
        RuntimeError: If the API request fails
    """
    # Get credentials from parameters or environment variables
    if username is None:
        try:
            username = os.environ['CEDA_USERNAME']
        except KeyError:
            raise KeyError("CEDA_USERNAME environment variable is required")

    if password is None:
        try:
            password = os.environ['CEDA_PASSWORD']
        except KeyError:
            raise KeyError("CEDA_PASSWORD environment variable is required")

    url = "https://services.ceda.ac.uk/api/token/create/"

    # Create basic auth token
    token = b64encode(f"{username}:{password}".encode('utf-8')).decode("ascii")
    headers = {
        "Authorization": f'Basic {token}',
    }

    print(headers)
    response = requests.post(url, headers=headers)
    

    if response.status_code == 200:
        response_data = json.loads(response.text)
        access_token = response_data["access_token"]
        print('[INFO] CEDA access token refreshed successfully.')
        return access_token
    else:
        raise RuntimeError(
            f"Failed to refresh token. Server responded with error code {response.status_code}: {response.text}"
        )


def setup_credentials():
    """
    Get CEDA access token.

    First attempts to load from CEDA_ACCESS_TOKEN environment variable.
    If not found, automatically generates a new token using CEDA_USERNAME
    and CEDA_PASSWORD environment variables.

    :return: Access token string
    """
    try:
        token = os.environ['CEDA_ACCESS_TOKEN']
        print('[INFO] CEDA access token loaded from environment.')
        return token
    except KeyError:
        print('[INFO] CEDA_ACCESS_TOKEN not found, generating new token...')
        return refresh_access_token()

def get_weather_data(url, access_token):
    """Gets weather csv from specified url and converts to pandas DataFrame.

    
    Details of how to find the url for the weather data can be found here:
        https://help.ceda.ac.uk/article/4442-ceda-opendap-scripted-interactions#findurl
    
    Args:
        url (string): Url from CEDA of .csv weather data.
        access_token (string): CEDA OAuth2 access token.

    Returns:
        pd.DataFrame: Data from .csv file with location column.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print(response.content.decode("utf8"))
        header, data = response.content.decode("utf8").split("\ndata\n")
        data = data.split("\nend data\n")[0]
        df = pd.read_csv(io.StringIO(data))
        matches = re.search("observation_station,\w+,(\w+)", header).groups()
        if len(matches) == 1:
            location = matches[0]
        else:
            raise RuntimeError("Multiple matches found for location in the csv.")
        df["weather_station"] = location

    else:
        raise RuntimeError(
            f"Server responded with error code {response.status_code}: {response.text}"
        )
    return df
