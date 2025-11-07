#!/usr/bin/env python

"""Merge multiple SpatialData objects into one"""

import os
from importlib.metadata import version
import spatialdata

PROCESS = "${task.process}"
PREFIX = "${prefix}"
SDATAS = "${sdata}"  # List of input SpatialData zarr paths

os.makedirs(PREFIX, exist_ok=True)
print(SDATAS)

# Read all zarr SpatialData directories
SDATAS = SDATAS if isinstance(SDATAS, list) else [SDATAS]
#
sdatas = []
for file in SDATAS:
    sdata = spatialdata.read_zarr(file)
    sdatas.append(sdata)

# Merge the data
output_sdata = spatialdata.concatenate(
    sdatas,
    region_key=None,
    instance_key=None,
    concatenate_tables=False,
    obs_names_make_unique=True,
    modify_tables_inplace=False,
)

# Save the concatenated data
output_sdata.write(f"{PREFIX}/merged_sdata.zarr", overwrite=True)

with open("versions.yml", "w", encoding="utf-8") as f:
    f.write(f"{PROCESS}:\\n")
    f.write(f"  spatialdata: {version('spatialdata')}\\n")
