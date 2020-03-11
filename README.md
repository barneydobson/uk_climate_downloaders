# haduk_downloader
This repository provides a python script that can automatically download and manipulate datasets from Metoffice's Had-UK website (https://catalogue.ceda.ac.uk/uuid/4dc8450d889a491ebb20e724debe2dfb). This dataset contains gridded estimates of historic climate data that covers the entire UK.

Users can specify time-series type, grid scale and time-step and automatically download this data (scripts/download_haduk_data.py)

Users can also aggregate the downloaded data over a specified shapefile (scripts/load_and_aggregate_data.py)

Required modules:

-To download: requests, pandas, os, certifi

-To aggregate: geopandas, shapely, xarray, netcdf4, pandas, numpy

-Also used (but you can change the code so these aren't required): tqdm, os

## DOI
[![DOI](https://zenodo.org/badge/228587225.svg)](https://zenodo.org/badge/latestdoi/228587225)
