#!/usr/bin/env python3
"""
Preprocess script for spatial-ir-pipeline
"""

# adapted from btc/spatial
from pathlib import Path

from cirro.helpers.preprocess_dataset import PreprocessDataset
import numpy as np
import pandas as pd


SAMPLESHEET_REQUIRED_COLUMNS = [
    "sampleid",
    "sample_path",
]
SAMPLESHEET_COLUMNS = ["sampleid", "sample_path", "align"]


def get_sample_paths(ds):
    """Create a samplesheet dataframe.
    Assumes params["cirro_input"] is a list of dicts with keys "name" and "s3".
    """
    if ds.files.empty:
        ds.logger.warning("No files found. Preparing samplesheet from dataset paths.")
        return pd.DataFrame(
            {
                "sampleid": [x["name"] for x in ds.params["cirro_input"]],
                "sample_path": [x["s3"] for x in ds.params["cirro_input"]],
            }
        )
    #
    # check if path from samplesheet is a directory or a file
    # if its a file (why not just use "."?)
    #   it associates sample with a file in the sample's root directory
    #   Convert s3 link to PosixPath and derive parent; convert back into string
    #   Path converts s3:// to s3:/, so revert proper s3 prefix afterwards
    # if directory
    #   and the directory name is the same as sample name
    #   then we can use that directory as sample_path
    # ds.files["sample_path"] = ds.files["file"].apply(
    #    lambda x: x if Path(x).is_dir() else str(Path(x).parent).replace("s3:/", "s3://")
    # )
    file1 = ds.files["file"].to_list()[0]

    ds.logger.info("exists %s: %s", file1, Path(file1).exists())
    ds.logger.info("is dir %s: %s", file1, Path(file1).is_dir())
    ds.logger.info("is file %s: %s", file1, Path(file1).is_file())

    ds.files["sample_path"] = ds.files["file"]
    ds.files["sampleid"] = ds.files["sample"]
    #
    ds.logger.info("Sample paths derived from dataset files")
    ds.logger.info("files: \n%s", ds.files["file"].to_list())
    ds.logger.info("sample paths:\n%s", ds.files["sample_path"].to_list())
    ds.logger.info("samples:\n%s", ds.files["sampleid"].to_list())
    #
    return pd.merge(ds.samplesheet, ds.files, on="sample", how="left")


def prepare_samplesheet(ds: PreprocessDataset) -> pd.DataFrame:
    """Set params as samplesheet.
    Assumes ds.params["cirro_input"] is a list of dicts with keys "name" and "s3".
    """
    ds.logger.info("params: %s", ds.params)
    ds.logger.info("files: %s", ds.files)
    ds.logger.info("samplesheet: %s", ds.samplesheet)
    #
    samplesheet = get_sample_paths(ds)
    #
    for k in SAMPLESHEET_COLUMNS:
        if k in ds.params.keys():
            samplesheet[k] = ds.params[k]

    # check is pipeline uses Cirro samplesheet, and if not prepare it from params
    if samplesheet.empty:
        raise ValueError("No files found in dataset and unable to prepare samplesheet from params.")

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
        if k in samplesheet.columns:
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
    raise SystemExit(0)


if __name__ == "__main__":
    main()
