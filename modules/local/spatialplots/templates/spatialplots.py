#!/usr/bin/env python3

"""
Generate spatial plots for IR data
"""

import os
from importlib.metadata import version

# import circuss as cs


PROCESS = "${task.process}"
PREFIX = "${prefix}"
SAMPLEID = "${meta.id}"

os.makedirs(PREFIX, exist_ok=True)

# versions
with open("versions.yml", "w", encoding="utf-8") as f:
    f.write(f"{PROCESS}:\\n")
    f.write(f"    circuss: {version('circuss')}\\n")
