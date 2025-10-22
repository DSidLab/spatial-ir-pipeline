#!/usr/bin/env python3

"""load Visium data"""

import os
import circuss as cs

PROCESS = "${task.process}"
PREFIX = "${prefix}"
SAMPLEID = "${meta.id}"

os.makedirs(PREFIX, exist_ok=True)
SPATIAL_RNA = "$spatial_rna" if "$spatial_rna" != "" else None

sdata = cs.io.load_visium(
    sampleid=SAMPLEID, sample_path="$sample_path", spatial_rna=SPATIAL_RNA, h5ad_file="$raw_adata_path"
)
# preprocess
cs.preprocessing.receptor_qc(sdata)
# add ext methods
sdata[f"{SAMPLEID}_rna"].write_h5ad(f"{PREFIX}/adata_pp.h5ad")

# versions
with open("versions.yml", "w", encoding="utf-8") as f:
    f.write(f"{PROCESS}:\\n")
    f.write("    circuss: 0.0.1dev\\n")
