#!/usr/bin/env python3

"""
Preprocess spatial IR data
"""

import os

# import circuss as cs

os.makedirs("${prefix}", exist_ok=True)
PROCESS = "${task.process}"

# versions
with open("versions.yml", "w", encoding="utf-8") as f:
    f.write(f"{PROCESS}:\\n")
    f.write("    circuss: 0.0.1dev\\n")
