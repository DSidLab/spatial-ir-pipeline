#!/usr/bin/env python3
"""
Preprocess script for spatial-ir-pipeline
"""

# From btc/spatial
from pathlib import Path
from urllib.parse import urlparse

from cirro.helpers.preprocess_dataset import PreprocessDataset
import numpy as np
import pandas as pd


SAMPLESHEET_REQUIRED_COLUMNS = [
    "sampleid",
    "sample_path",
]
SAMPLESHEET_COLUMNS = ["sampleid", "sample_path", "align"]


def is_url(string):
    """Helper function to check if a string is a URL."""
    try:
        result = urlparse(string)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def samplesheet_from_files(params, ds):
    """Create a samplesheet dataframe.
    Assumes params["cirro_input"] is a list of dicts with keys "name" and "s3".
    """
    pipeline_params = {k: [params[k]] for k in SAMPLESHEET_COLUMNS if k in params.keys()}

    files = ds.files

    # Assumes samplesheet associates sample with a file in the sample's root directory
    # Convert s3 link to PosixPath and derive parent; convert back into string
    # Path converts s3:// to s3:/, so revert proper s3 prefix afterwards
    files["sample_path"] = files["file"].apply(lambda x: str(Path(x).parent).replace("s3:/", "s3://"))
    files = files[["sample", "sample_path"]]

    data_params = pd.merge(ds.samplesheet, files, on="sample", how="left")
    return data_params.join(pd.DataFrame(pipeline_params), how="cross")


def samplesheet_from_params(params):
    """Create a samplesheet dataframe from params."""

    return pd.DataFrame(
        {
            "sample": [x["name"] for x in params["cirro_input"]],
            "data_directory": [x["s3"] + "/data" for x in params["cirro_input"]],
        }
    )


def prepare_samplesheet(ds: PreprocessDataset) -> pd.DataFrame:
    """Set params as samplesheet.
    Assumes ds.params["cirro_input"] is a list of dicts with keys "name" and "s3".
    """
    ds.logger.info([ds.params])

    samplesheet = samplesheet_from_files(ds.params, ds)

    # check is pipeline uses Cirro samplesheet, and if not prepare it from params
    if samplesheet.empty:
        ds.logger.warning("No files found in dataset. Preparing samplesheet from params.")
        samplesheet = samplesheet_from_params(ds.params)
        if samplesheet.empty:
            raise ValueError("No files found in dataset and unable to prepare samplesheet from params.")
        ds.logger.info("Prepared samplesheet from params.")

    for colname in SAMPLESHEET_REQUIRED_COLUMNS:
        if colname not in samplesheet.columns:
            ds.logger.warning(f"Samplesheet is missing required column '{colname}'. Populating with NaN.")
            samplesheet[colname] = np.nan

    for colname in samplesheet.columns:
        if colname not in SAMPLESHEET_COLUMNS:
            del samplesheet[colname]

    # Save to a file
    samplesheet.to_csv("cirro-samplesheet.csv", index=None)

    # Clear params that we wrote to the samplesheet
    # cleared params will not overload the nextflow.params
    to_remove = []
    for k in ds.params:
        if k in SAMPLESHEET_COLUMNS:
            to_remove.append(k)

    for k in to_remove:
        ds.remove_param(k)

    ds.add_param("input", "cirro-samplesheet.csv")

    # Log the samplesheet
    ds.logger.info(samplesheet.to_dict())


def main():
    """Main function."""
    ds = PreprocessDataset.from_running()

    prepare_samplesheet(ds)

    # log
    ds.logger.info(ds.params)
    print(ds.params)


if __name__ == "__main__":
    main()
