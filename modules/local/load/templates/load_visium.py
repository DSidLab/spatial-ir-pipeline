#!/usr/bin/env python3

"""load Visium data"""

import os
from importlib.metadata import version

import circuss as cs

PROCESS = "${task.process}"
PREFIX = "${prefix}"
SAMPLEID = "${meta.id}"

os.makedirs(PREFIX, exist_ok=True)
SPATIAL_RNA = "$spatial_rna" if "$spatial_rna" != "" else None
CLN_OUTPUT = "$clonotype_output" if "$clonotype_output" != "" else None

sdata = cs.io.load_visium(
    sampleid=SAMPLEID, sample_path="$sample_path", spatial_rna=SPATIAL_RNA, clonotype_output=CLN_OUTPUT
)
sdata[f"{SAMPLEID}_rna"].write_h5ad(f"{PREFIX}/adata.h5ad")

# versions
with open("versions.yml", "w", encoding="utf-8") as f:
    f.write(f"{PROCESS}:\\n")
    f.write(f"    circuss: {version('circuss')}\\n")
