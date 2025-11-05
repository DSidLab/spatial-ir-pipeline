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


def get_sample_paths(samples, files, logger=None) -> pd.DataFrame:
    """
    Create a samplesheet dataframe.
    """
    #
    fastq_files = files[files["file"].str.contains(".fastq.gz")]
    fastq_files = (
        fastq_files.assign(pos=fastq_files.groupby("sample").cumcount())
        .pivot(index="sample", columns="pos", values="file")
        .rename(columns=lambda i: f"file_{i}")
        .reset_index()
    )
    #
    fastq_columns = fastq_files.columns.tolist()
    fastq_columns.remove("sample")
    fastq_files["ir_fastq_path"] = fastq_files[fastq_columns[0]].apply(
        lambda x: str(Path(x).parent).replace("s3:/", "s3://")
    )
    #
    fastq_files["ir_read_ids"] = (
        fastq_files[fastq_columns]
        .map(lambda p: Path(str(p)).name)
        .apply(lambda row: ",".join(x for x in row if x), axis=1)
    )
    fastq_files.drop(columns=fastq_columns, inplace=True)
    #
    sample_rna = files.loc[files["file"].str.contains(".h5")].copy()
    sample_rna["spatial_rna"] = sample_rna["file"].apply(lambda x: str(Path(x).parent).replace("s3:/", "s3://"))
    sample_rna["sample_path"] = sample_rna["spatial_rna"].apply(lambda x: str(Path(x).parent).replace("s3:/", "s3://"))
    cols_to_drop = [col for col in sample_rna.columns if col not in ["sample", "spatial_rna", "sample_path"]]
    sample_rna.drop(columns=cols_to_drop, inplace=True)
    #
    clonotype_data = files[files["file"].str.contains("clonotype_output")].copy()
    clonotype_data["clonotype_output"] = clonotype_data["file"].apply(
        lambda x: str(Path(x).parent).replace("s3:/", "s3://")
    )
    clonotype_data = (
        clonotype_data.groupby("sample")["clonotype_output"]
        .apply(lambda x: ",".join(map(str, x.dropna())))
        .reset_index()
    )
    #
    files = pd.merge(
        pd.merge(sample_rna, fastq_files, on="sample", how="left"), clonotype_data, on="sample", how="left"
    )
    files["sampleid"] = files["sample"]
    #
    if logger is not None:
        logger.info("Sample paths derived from dataset files")
        logger.info("sample paths:\n%s", files["sample_path"].to_list())
        logger.info("samples:\n%s", files["sampleid"].to_list())
    #
    return pd.merge(samples, files, on="sample", how="left")


def prepare_samplesheet(ds: PreprocessDataset) -> pd.DataFrame:
    """Set params as samplesheet.
    Assumes ds.params["cirro_input"] is a list of dicts with keys "name" and "s3".
    """
    ds.logger.info("params: %s", ds.params)
    ds.logger.info("files: %s", ds.files)
    ds.logger.info("samplesheet: %s", ds.samplesheet)
    #
    if ds.files.empty:
        ds.logger.warning("No files found. Preparing samplesheet from dataset paths.")
        samplesheet = pd.DataFrame(
            {
                "sampleid": [x["name"] for x in ds.params["cirro_input"]],
                "sample_path": [x["s3"] for x in ds.params["cirro_input"]],
            }
        )
    else:
        samplesheet = get_sample_paths(ds.samplesheet, ds.files, ds.logger)
    #
    for k in SAMPLESHEET_COLUMNS:
        if k in ds.params.keys():
            samplesheet[k] = ds.params[k]
            ds.remove_param(k)

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
    #
    ds.remove_param("cirro_input")

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
