"""Test script for preprocess.py"""

import unittest
from io import StringIO

import pandas

from preprocess import get_sample_paths


TEST_FILES = """
sample,file
emory_pt15,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt15/spatial_ir/clonotype_output/parameter_set_1/emory_pt15.spatial.airr.tsv
emory_pt26,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt26/spatial_rna/filtered_feature_bc_matrix.h5
emory_pt19,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt19/spatial_ir/ir_fastq/SRR15052440_1.fastq.gz
emory_pt26,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt26/spatial_ir/clonotype_output/parameter_set_1/emory_pt26.spatial.airr.tsv
emory_pt19,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt19/spatial_ir/ir_fastq/SRR15052440_2.fastq.gz
emory_pt27,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt27/spatial_ir/clonotype_output/parameter_set_1/emory_pt27.spatial.airr.tsv
emory_pt24,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt24/spatial_ir/clonotype_output/parameter_set_1/emory_pt24.spatial.airr.tsv
emory_pt16,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt16/spatial_rna/filtered_feature_bc_matrix.h5
emory_pt16,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt16/spatial_ir/ir_fastq/SRR15052441_2.fastq.gz
emory_pt15,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt15/spatial_rna/filtered_feature_bc_matrix.h5
emory_pt16,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt16/spatial_ir/ir_fastq/SRR15052441_1.fastq.gz
emory_pt27,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt27/spatial_rna/filtered_feature_bc_matrix.h5
emory_pt19,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt19/spatial_ir/clonotype_output/parameter_set_1/emory_pt19.spatial.airr.tsv
emory_pt24,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt24/spatial_ir/ir_fastq/SRR15052439_1.fastq.gz
emory_pt26,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt26/spatial_ir/ir_fastq/SRR15052438_2.fastq.gz
emory_pt27,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt27/spatial_ir/ir_fastq/SRR15052437_2.fastq.gz
emory_pt26,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt26/spatial_ir/ir_fastq/SRR15052438_1.fastq.gz
emory_pt16,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt16/spatial_ir/clonotype_output/parameter_set_1/emory_pt16.spatial.airr.tsv
emory_pt24,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt24/spatial_rna/filtered_feature_bc_matrix.h5
emory_pt15,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt15/spatial_ir/ir_fastq/SRR15052442_1.fastq.gz
emory_pt24,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt24/spatial_ir/ir_fastq/SRR15052439_2.fastq.gz
emory_pt19,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt19/spatial_rna/filtered_feature_bc_matrix.h5
emory_pt27,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt27/spatial_ir/ir_fastq/SRR15052437_1.fastq.gz
emory_pt15,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt15/spatial_ir/ir_fastq/SRR15052442_2.fastq.gz
""".strip()

TEST_SAMPLESHEET = """
sample
emory_pt15
emory_pt16
emory_pt19
emory_pt24
emory_pt26
emory_pt27
""".strip()

TEST_MANIFEST = """
sample,spatial_rna,sample_path,ir_fastq_path,ir_read_ids,clonotype_output,sampleid
emory_pt15,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt15/spatial_rna,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt15,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt15/spatial_ir/ir_fastq,"SRR15052442_1.fastq.gz,SRR15052442_2.fastq.gz",s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt15/spatial_ir/clonotype_output/parameter_set_1,emory_pt15
emory_pt16,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt16/spatial_rna,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt16,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt16/spatial_ir/ir_fastq,"SRR15052441_2.fastq.gz,SRR15052441_1.fastq.gz",s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt16/spatial_ir/clonotype_output/parameter_set_1,emory_pt16
emory_pt19,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt19/spatial_rna,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt19,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt19/spatial_ir/ir_fastq,"SRR15052440_1.fastq.gz,SRR15052440_2.fastq.gz",s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt19/spatial_ir/clonotype_output/parameter_set_1,emory_pt19
emory_pt24,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt24/spatial_rna,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt24,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt24/spatial_ir/ir_fastq,"SRR15052439_1.fastq.gz,SRR15052439_2.fastq.gz",s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt24/spatial_ir/clonotype_output/parameter_set_1,emory_pt24
emory_pt26,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt26/spatial_rna,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt26,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt26/spatial_ir/ir_fastq,"SRR15052438_2.fastq.gz,SRR15052438_1.fastq.gz",s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt26/spatial_ir/clonotype_output/parameter_set_1,emory_pt26
emory_pt27,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt27/spatial_rna,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt27,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt27/spatial_ir/ir_fastq,"SRR15052437_2.fastq.gz,SRR15052437_1.fastq.gz",s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/28d3544d-f5c8-4041-a897-531c283a915f/data/emory_pt27/spatial_ir/clonotype_output/parameter_set_1,emory_pt27
""".strip()


class PreprocessTest(unittest.TestCase):
    """
    Tests for preprocess.py
    """

    def test_formats_samplesheet(self):
        """Test get_sample_paths function"""
        manifest = get_sample_paths(
            samples=pandas.read_csv(StringIO(TEST_SAMPLESHEET)), files=pandas.read_csv(StringIO(TEST_FILES))
        )
        manifest_csv = manifest.to_csv(index=False).strip()
        self.assertEqual(manifest_csv, TEST_MANIFEST)
