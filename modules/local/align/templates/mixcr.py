#!/usr/bin/env python3

"""Align immune receptor sequences using MiXCR"""

import os
import subprocess
import re

import circuss as cs
from circuss.logging import circuss_logger

circuss_logger.setLevel("DEBUG")

PROCESS = "${task.process}"
PREFIX = "${prefix}"

os.makedirs(PREFIX, exist_ok=True)

n_read_ids = len("$ir_read_ids".split(","))
if n_read_ids % 2 != 0:
    raise ValueError(f"Number of IR read IDs must be even (paired-end reads). Found: {n_read_ids}")
n_pairs = n_read_ids // 2
if n_pairs < 1:
    raise ValueError("At least one pair of IR read IDs must be provided for alignment.")

circuss_logger.debug("ir fastq path: $ir_fastq_path")
# pylint: disable=comparison-of-constants
IR_FASTQ_PATH = "$ir_fastq_path" if "$ir_fastq_path" != "" else None

clonotype_outputs = []
for clonotype_id in range(n_pairs):
    clonotype_output = f"{PREFIX}/clonotype_output/{clonotype_id + 1}"
    clonotype_outputs.append(clonotype_output)

cs.align.run_mixcr(
    sampleid="$meta.id",
    sample_path="$sample_path",
    ir_read_ids="$ir_read_ids",
    ir_fastq_path=IR_FASTQ_PATH,
    clonotype_output=clonotype_outputs,
)

out = subprocess.check_output(["mixcr", "-v"], stderr=subprocess.STDOUT, text=True).splitlines()[0]
mixcr_version = re.search(r"MiXCR v([0-9.]+)", out, flags=re.IGNORECASE).group(1)

with open("versions.yml", "w", encoding="utf-8") as f:
    f.write(f"{PROCESS}:\\n")
    f.write("  circuss: 0.0.1dev\\n")
    f.write(f"  mixcr: {mixcr_version}\\n")
