#!/usr/bin/env python3

"""load Visium data"""

import os
from importlib.metadata import version

import circuss as cs
import numpy as np
from sidus import externals as ext

PROCESS = "${task.process}"
PREFIX = "${prefix}"
SAMPLEID = "${meta.id}"

os.makedirs(PREFIX, exist_ok=True)
SPATIAL_RNA = "$spatial_rna" if "$spatial_rna" != "" else None
CLN_OUTPUT = "$clonotype_output" if "$clonotype_output" not in ("", "[]") else "1"
CELL_ANNS = "$cell_annotations" if "$cell_annotations" != "" else None
print(CLN_OUTPUT)
print(SPATIAL_RNA)

sdata = cs.io.load_visium(
    sampleid=SAMPLEID,
    sample_path="$sample_path",
    spatial_rna=SPATIAL_RNA,
    clonotype_output=CLN_OUTPUT,
    cell_annotations=CELL_ANNS,
)
sdata[f"{SAMPLEID}_rna"].write_h5ad(f"{PREFIX}/adata_raw.h5ad")
# preprocess
cs.preprocessing.receptor_qc(sdata)
ext.preprocess(sdata, filter_housekeeping=True)  # hb, mito, ribo - if set to false will generate qc stats
# ext.sctransform(ir_data)
ext.basic_scanpy(sdata)
ext.cluster_leiden(sdata)
# ext.spagcn(ir_data) # package uses sc.tl.louvian which has been removed
ext.rank_groups_and_get_dendrogram(sdata)  # , layer = 'data')
sdata[f"{SAMPLEID}_rna"].uns["leiden"]["params"]["random_state"] = 0
# add ext methods
cs.analysis.get_ir_alpha_diversity(sdata, obs_mask="leiden")
# save ir diversity (recarry doesn't serialize)
np.save(f"{PREFIX}/ir_diversity_data.npy", sdata[f"{SAMPLEID}_rna"].uns["ir_diversity"])
del sdata[f"{SAMPLEID}_rna"].uns["ir_diversity"]
#
sdata[f"{SAMPLEID}_rna"].write_h5ad(f"{PREFIX}/adata_pp.h5ad")

# versions
with open("versions.yml", "w", encoding="utf-8") as f:
    f.write(f"{PROCESS}:\\n")
    f.write(f"    circuss: {version('circuss')}\\n")
    f.write(f"    sidus: {version('sidus')}\\n")
