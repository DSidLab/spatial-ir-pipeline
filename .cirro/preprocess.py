#!/usr/bin/env python3
"""
Preprocess script for spatial-ir-pipeline
"""

from pathlib import Path

from cirro.helpers.preprocess_dataset import PreprocessDataset
import numpy as np
import pandas as pd


SAMPLESHEET_REQUIRED_COLUMNS = [
    "sampleid",
    "sample_path",
]
SAMPLESHEET_COLUMNS = ["sampleid", "sample_path", "align"]


def _get_tcrseq_files(samples, files) -> pd.DataFrame:
    """
    Create a samplesheet dataframe with fastq files.
    """
    tcrseq_files = files.loc[files["process"] == "tcrseq"].copy()
    tcrseq_files = (
        tcrseq_files.assign(pos=tcrseq_files.groupby("sample").cumcount())
        .pivot(index="sample", columns="pos", values="file")
        .rename(columns=lambda i: f"file_{i}")
        .reset_index()
    )
    #
    tcrseq_columns = tcrseq_files.columns.tolist()
    tcrseq_columns = [col for col in tcrseq_columns if col.startswith("file_")]
    tcrseq_files["ir_fastq_path"] = tcrseq_files[tcrseq_columns[0]].apply(
        lambda x: str(Path(x).parent).replace("s3:/", "s3://")
    )
    #
    tcrseq_files["ir_read_ids"] = (
        tcrseq_files[tcrseq_columns]
        .map(lambda p: Path(str(p)).name)
        .apply(lambda row: ",".join(x for x in row if x), axis=1)
    )
    #
    tcrseq_files = pd.merge(samples, tcrseq_files, on="sample", how="left")
    tcrseq_files.drop(columns=tcrseq_columns + ["sample"], inplace=True)
    #
    return tcrseq_files


def _get_visium_files(samples, files) -> pd.DataFrame:
    """
    Create a samplesheet dataframe with rna files.
    """
    sample_rna = files.loc[files["process"] == "ingest_spaceranger"].copy()
    cols_to_drop = sample_rna.columns.tolist()
    sample_rna["spatial_rna"] = sample_rna["file"].apply(lambda x: str(Path(x).parent).replace("s3:/", "s3://"))
    sample_rna["sample_path"] = sample_rna["spatial_rna"].apply(lambda x: str(Path(x).parent).replace("s3:/", "s3://"))
    #
    sample_rna = pd.merge(samples, sample_rna, on="sample", how="left")
    sample_rna.drop(columns=cols_to_drop, inplace=True)
    #
    return sample_rna


def _get_clonotype_files(samples, files) -> pd.DataFrame:
    """
    Create a samplesheet dataframe with clonotype files.
    """
    clonotype_data = files[files["process"] == "table_csv_tsv"].copy()
    clonotype_data["clonotype_output"] = clonotype_data["file"].apply(
        lambda x: str(Path(x).parent).replace("s3:/", "s3://")
    )
    clonotype_data = (
        clonotype_data.groupby("sample")["clonotype_output"]
        .apply(lambda x: ",".join(map(str, x.dropna())))
        .reset_index()
    )
    #
    clonotype_data = pd.merge(samples, clonotype_data, on="sample", how="left")
    #
    clonotype_data.drop(columns="sample", inplace=True)
    #
    return clonotype_data


def get_sample_paths(samples, files, sample_label, logger=None) -> pd.DataFrame:
    """
    Create a samplesheet dataframe.
    """
    samples["sampleid"] = samples[sample_label]
    samples = samples[["sample", "sampleid"]].drop_duplicates().copy()
    #
    tcrseq_files = _get_tcrseq_files(samples, files)
    visium_files = _get_visium_files(samples, files)
    clonotype_files = _get_clonotype_files(samples, files)
    #
    files = pd.merge(visium_files, tcrseq_files, on=["sampleid"], how="outer")
    files = pd.merge(files, clonotype_files, on=["sampleid"], how="outer")
    #
    if logger is not None:
        logger.info("Sample paths derived from dataset files")
        logger.info("samples:\n%s", files["sampleid"].to_list())
    #
    return files


def prepare_samplesheet(ds: PreprocessDataset) -> pd.DataFrame:
    """Set params as samplesheet.
    Assumes ds.params["cirro_input"] is a list of dicts with keys "name" and "s3".
    """
    ds.logger.info("params: %s", ds.params)
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
        samplesheet = get_sample_paths(ds.samplesheet, ds.files, ds.params.sample_label, ds.logger)
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
