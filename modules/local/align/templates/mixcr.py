#!/usr/bin/env python3

"""Align immune receptor sequences using MiXCR"""

import os
import circuss as cs

PROCESS = "${task.process}"
PREFIX = "${prefix}"
os.makedirs(PREFIX, exist_ok=True)

n_read_ids = len("$ir_read_ids".split(","))
if n_read_ids % 2 != 0:
    raise ValueError(f"Number of IR read IDs must be even (paired-end reads). Found: {n_read_ids}")
n_pairs = n_read_ids // 2
if n_pairs < 1:
    raise ValueError("At least one pair of IR read IDs must be provided for alignment.")

clonotype_outputs = []
for clonotype_id in range(n_pairs):
    clonotype_output = f"{PREFIX}/$sampleid/spatial_ir/clonotype_output/{clonotype_id + 1}"
    clonotype_outputs.append(clonotype_output)

cs.align.run_mixcr(
    sampleid="$sampleid",
    sample_path="$sample_path",
    ir_read_ids="$ir_read_ids",
    ir_fastq_path="$ir_fastq_path",
    clonotype_output=clonotype_outputs,
    mixcr_path="/opt/conda/pkgs/",
    mixcr_version="4.7.0-0",
    dry_run=True,
)

# versions
with open("versions.yml", "w", encoding="utf-8") as f:
    f.write(f"{PROCESS}:\\n")
    f.write("    circuss: 0.0.1dev\\n")
