#!/usr/bin/env python3

"""
Generate ir summary output tables for IR data
"""

import os
from importlib.metadata import version

import circuss as cs
import numpy as np
import pandas as pd


PROCESS = "${task.process}"
PREFIX = "${prefix}"
SAMPLEID = "${meta.id}"


os.makedirs(f"{PREFIX}/outs", exist_ok=True)
SPATIAL_RNA = "$spatial_rna" if "$spatial_rna" != "" else None

sdata = cs.io.load_visium(
    sampleid=SAMPLEID,
    sample_path="$sample_path",
    spatial_rna=SPATIAL_RNA,
    h5ad_file="$adata_pp",
)
sdata[f"{SAMPLEID}_rna"].uns["ir_diversity"] = np.load("$ir_diversity_data", allow_pickle=True)
adata = sdata[f"{SAMPLEID}_rna"]
# generate ir summary
report = pd.DataFrame(
    {
        "Sample": [SAMPLEID],
        "n_genes": adata.shape[1],
        "n_cells": adata.shape[0],
        "mean_genes_by_counts": adata.obs["n_genes_by_counts"].mean(),
        "mean_cells_by_counts": adata.var["n_cells_by_counts"].mean(),
        "mean_total_nnz_counts": adata.layers["counts"][adata.layers["counts"].nonzero()].mean(),
        "mean_unique_irs_by_counts": adata.obs["n_IR_by_obs"].mean(),
        "mean_total_irs_by_counts": adata.obs["total_IR_by_obs"].mean(),
    }
)
report.to_csv(f"{PREFIX}/outs/ir_summary_report.csv", index=False)

# save diversity report
diversity_report = pd.DataFrame.from_records(adata.uns["ir_diversity"])
diversity_report.insert(loc=0, column="Sample", value=SAMPLEID)
diversity_report["chao1_ci"] = diversity_report["chao1_ci"].apply(
    lambda v: str(v) if isinstance(v, int) else f"{round(v[0])}-{round(v[1])}"
)
diversity_report.to_csv(f"{PREFIX}/outs/ir_diversity_report.csv", index=False)

# versions
with open("versions.yml", "w", encoding="utf-8") as f:
    f.write(f"{PROCESS}:\\n")
    f.write(f"    circuss: {version('circuss')}\\n")
