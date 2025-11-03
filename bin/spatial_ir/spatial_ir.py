"""
Spatial IR Module for MultiQC
"""

import json
import logging

from multiqc.base_module import BaseMultiqcModule, ModuleNoSamplesFound

log = logging.getLogger("multiqc")


# pylint: disable=R1725
class MultiqcModule(BaseMultiqcModule):
    """
    MultiQC module for Spatial IR Pipeline results.
    """

    def __init__(self):
        super(MultiqcModule, self).__init__(
            name="Spatial IR Pipeline",
            target="spatial_ir",
            anchor="spatial_ir",
            info="Module for analyzing Spatial IR Pipeline results.",
        )

        data_by_sample = {}
        for f in self.find_log_files("spatial_ir/ir_summary", filehandles=True):
            s_name = f["s_name"]
            if s_name in data_by_sample:
                log.debug("Duplicate sample name found! Overwriting: %s", s_name)
            parsed_data = json.loads(f["f"])
            if parsed_data:
                # Use parent directory name as sample name
                data_by_sample[s_name] = parsed_data[s_name]
                self.add_data_source(f, s_name, section="IR Summary")

        data_by_sample = self.ignore_samples(data_by_sample)

        if len(data_by_sample) == 0:
            raise ModuleNoSamplesFound

        log.info("Found %d reports", len(data_by_sample))

        # Superfluous function call to confirm that it is used in this module
        # Replace None with actual version if it is available
        self.add_software_version(None)

        # Basic Stats Table
        self.spatial_ir_general_stats_table(data_by_sample)

        # Diversity Plot
        # self.add_section(plot=self.spatialir_diversity_plot(data_by_sample))

        # Write parsed report data to a file
        self.write_data_file(data_by_sample, "multiqc_spatialir_summary")

    def spatial_ir_general_stats_table(self, data_by_sample):
        """Take the parsed stats from the Spatial IR report and add it to the
        basic stats table at the top of the report"""

        headers = {
            "Sample": {
                "title": "Sample",
                "description": "Sample ID",
            },
            "n_genes": {
                "title": "N Genes",
                "description": "Number of genes detected",
                "min": 0,
                "suffix": " genes",
                "scale": "RdYlGn",
            },
            "n_cells": {
                "title": "N Cells",
                "description": "Number of cells detected",
                "min": 0,
                "suffix": " cells",
                "scale": "RdYlGn",
            },
            "mean_genes_by_counts": {
                "title": "Mean Genes by Counts",
                "description": "Mean number of genes detected per cell",
                "min": 0,
                "scale": "PuRd",
            },
            "mean_cells_by_counts": {
                "title": "Mean Cells by Counts",
                "description": "Mean number of cells detected per gene",
                "min": 0,
                "scale": "PuRd",
            },
            "mean_total_nnz_counts": {
                "title": "Mean Total Non-Zero Counts",
                "description": "Mean number of total non-zero counts per cell",
                "min": 0,
                "scale": "PuRd",
            },
            "mean_unique_irs_by_counts": {
                "title": "Mean Unique IRs by Counts",
                "description": "Mean number of unique IRs detected per cell",
                "min": 0,
                "scale": "PuRd",
            },
            "mean_total_irs_by_counts": {
                "title": "Mean Total IRs by Counts",
                "description": "Mean number of total IRs detected per cell",
                "min": 0,
                "scale": "PuRd",
            },
        }
        self.general_stats_addcols(data_by_sample, headers)
