#!/usr/bin/env python3
"""
Preprocess script for spatial-ir-pipeline
"""

# From btc/spatial
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from cirro.helpers.preprocess_dataset import PreprocessDataset

SAMPLESHEET_REQUIRED_COLUMNS = (
    "sample",
    "data_directory",
)


# Helper function to check if a string is a URL
def is_url(string):
    """Check if a string is a URL."""
    try:
        result = urlparse(string)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def set_params_as_samplesheet(ds: PreprocessDataset) -> pd.DataFrame:
    """Set params as samplesheet.
    Assumes ds.params["cirro_input"] is a list of dicts with keys "name" and "s3".
    """
    ds.logger.info([ds.params])

    samplesheet = df_from_params(ds.params)

    for colname in SAMPLESHEET_REQUIRED_COLUMNS:
        if colname not in samplesheet.columns:
            samplesheet[colname] = np.nan

    for colname in samplesheet.columns:
        if colname not in SAMPLESHEET_REQUIRED_COLUMNS:
            del samplesheet[colname]

    # Save to a file
    samplesheet.to_csv("samplesheet.csv", index=None)

    # Clear params that we wrote to the samplesheet
    # cleared params will not overload the nextflow.params
    to_remove = []
    for k in ds.params:
        if k in SAMPLESHEET_REQUIRED_COLUMNS:
            to_remove.append(k)

    for k in to_remove:
        ds.remove_param(k)

    ds.add_param("input", "samplesheet.csv")

    # Log the samplesheet
    ds.logger.info(samplesheet.to_dict())


def df_from_params(params):
    """Create a samplesheet dataframe from params.
    Assumes params["cirro_input"] is a list of dicts with keys "name" and "s3".
    """
    pipeline_param_names = list(SAMPLESHEET_REQUIRED_COLUMNS)
    pipeline_params = {k: [params[k]] for k in pipeline_param_names if k in params.keys()}

    data_params = pd.DataFrame(
        {
            "sample": [x["name"] for x in params["cirro_input"]],
            "data_directory": [x["s3"] + "/data" for x in params["cirro_input"]],
        }
    )

    samplesheet = data_params.join(pd.DataFrame(pipeline_params), how="cross")

    return samplesheet


def main():
    """Main function."""
    ds = PreprocessDataset.from_running()

    set_params_as_samplesheet(ds)

    # log
    ds.logger.info(ds.params)
    print(ds.params)


if __name__ == "__main__":
    main()
