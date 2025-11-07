"""Test script for preprocess.py"""

import unittest
from io import StringIO

import pandas

from preprocess import get_sample_paths


TEST_FILES = """
sample,file,process,dataset,sampleIndex,read,readType
emory_pt19,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt19/spatial_ir/ir_fastq/SRR15052440_R1.fastq.gz,tcrseq,4c324020-84b8-474c-9a55-7736554585c4,3,1.0,R
emory_pt15,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt15/spatial_ir/ir_fastq/SRR15052442_R1.fastq.gz,tcrseq,4c324020-84b8-474c-9a55-7736554585c4,1,1.0,R
emory_pt16,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt16/spatial_ir/ir_fastq/SRR15052441_R1.fastq.gz,tcrseq,4c324020-84b8-474c-9a55-7736554585c4,2,1.0,R
emory_pt27,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt27/spatial_ir/ir_fastq/SRR15052437_R2.fastq.gz,tcrseq,4c324020-84b8-474c-9a55-7736554585c4,6,2.0,R
emory_pt26,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt26/spatial_ir/ir_fastq/SRR15052438_R1.fastq.gz,tcrseq,4c324020-84b8-474c-9a55-7736554585c4,5,1.0,R
emory_pt15,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt15/spatial_ir/ir_fastq/SRR15052442_R2.fastq.gz,tcrseq,4c324020-84b8-474c-9a55-7736554585c4,1,2.0,R
emory_pt16,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt16/spatial_ir/ir_fastq/SRR15052441_R2.fastq.gz,tcrseq,4c324020-84b8-474c-9a55-7736554585c4,2,2.0,R
emory_pt24,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt24/spatial_ir/ir_fastq/SRR15052439_R2.fastq.gz,tcrseq,4c324020-84b8-474c-9a55-7736554585c4,4,2.0,R
emory_pt24,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt24/spatial_ir/ir_fastq/SRR15052439_R1.fastq.gz,tcrseq,4c324020-84b8-474c-9a55-7736554585c4,4,1.0,R
emory_pt27,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt27/spatial_ir/ir_fastq/SRR15052437_R1.fastq.gz,tcrseq,4c324020-84b8-474c-9a55-7736554585c4,6,1.0,R
emory_pt19,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt19/spatial_ir/ir_fastq/SRR15052440_R2.fastq.gz,tcrseq,4c324020-84b8-474c-9a55-7736554585c4,3,2.0,R
emory_pt26,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt26/spatial_ir/ir_fastq/SRR15052438_R2.fastq.gz,tcrseq,4c324020-84b8-474c-9a55-7736554585c4,5,2.0,R
emory_pt24,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt24/spatial_rna/filtered_feature_bc_matrix.h5,ingest_spaceranger,2b05144b-5cbe-4db4-8d83-dcc61aa8b983,4,,
emory_pt16,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt16/spatial_rna/filtered_feature_bc_matrix.h5,ingest_spaceranger,2b05144b-5cbe-4db4-8d83-dcc61aa8b983,2,,
emory_pt26,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt26/spatial_rna/filtered_feature_bc_matrix.h5,ingest_spaceranger,2b05144b-5cbe-4db4-8d83-dcc61aa8b983,5,,
emory_pt15,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt15/spatial_rna/filtered_feature_bc_matrix.h5,ingest_spaceranger,2b05144b-5cbe-4db4-8d83-dcc61aa8b983,1,,
emory_pt19,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt19/spatial_rna/filtered_feature_bc_matrix.h5,ingest_spaceranger,2b05144b-5cbe-4db4-8d83-dcc61aa8b983,3,,
emory_pt27,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt27/spatial_rna/filtered_feature_bc_matrix.h5,ingest_spaceranger,2b05144b-5cbe-4db4-8d83-dcc61aa8b983,6,,
emory_pt15,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/ca89a503-047d-4961-b44f-1c02f85dcd0f/data/emory_pt15/spatial_ir/clonotype_output/parameter_set_1/emory_pt15.spatial.airr.tsv,table_csv_tsv,ca89a503-047d-4961-b44f-1c02f85dcd0f,1,,
emory_pt19,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/ca89a503-047d-4961-b44f-1c02f85dcd0f/data/emory_pt19/spatial_ir/clonotype_output/parameter_set_1/emory_pt19.spatial.airr.tsv,table_csv_tsv,ca89a503-047d-4961-b44f-1c02f85dcd0f,3,,
emory_pt26,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/ca89a503-047d-4961-b44f-1c02f85dcd0f/data/emory_pt26/spatial_ir/clonotype_output/parameter_set_1/emory_pt26.spatial.airr.tsv,table_csv_tsv,ca89a503-047d-4961-b44f-1c02f85dcd0f,5,,
emory_pt16,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/ca89a503-047d-4961-b44f-1c02f85dcd0f/data/emory_pt16/spatial_ir/clonotype_output/parameter_set_1/emory_pt16.spatial.airr.tsv,table_csv_tsv,ca89a503-047d-4961-b44f-1c02f85dcd0f,2,,
emory_pt24,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/ca89a503-047d-4961-b44f-1c02f85dcd0f/data/emory_pt24/spatial_ir/clonotype_output/parameter_set_1/emory_pt24.spatial.airr.tsv,table_csv_tsv,ca89a503-047d-4961-b44f-1c02f85dcd0f,4,,
emory_pt27,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/ca89a503-047d-4961-b44f-1c02f85dcd0f/data/emory_pt27/spatial_ir/clonotype_output/parameter_set_1/emory_pt27.spatial.airr.tsv,table_csv_tsv,ca89a503-047d-4961-b44f-1c02f85dcd0f,6,,
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
sampleid,spatial_rna,sample_path,ir_fastq_path,ir_read_ids,clonotype_output
emory_pt15,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt15/spatial_rna,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt15,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt15/spatial_ir/ir_fastq,"SRR15052442_R1.fastq.gz,SRR15052442_R2.fastq.gz",s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/ca89a503-047d-4961-b44f-1c02f85dcd0f/data/emory_pt15/spatial_ir/clonotype_output/parameter_set_1
emory_pt16,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt16/spatial_rna,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt16,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt16/spatial_ir/ir_fastq,"SRR15052441_R1.fastq.gz,SRR15052441_R2.fastq.gz",s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/ca89a503-047d-4961-b44f-1c02f85dcd0f/data/emory_pt16/spatial_ir/clonotype_output/parameter_set_1
emory_pt19,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt19/spatial_rna,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt19,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt19/spatial_ir/ir_fastq,"SRR15052440_R1.fastq.gz,SRR15052440_R2.fastq.gz",s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/ca89a503-047d-4961-b44f-1c02f85dcd0f/data/emory_pt19/spatial_ir/clonotype_output/parameter_set_1
emory_pt24,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt24/spatial_rna,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt24,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt24/spatial_ir/ir_fastq,"SRR15052439_R2.fastq.gz,SRR15052439_R1.fastq.gz",s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/ca89a503-047d-4961-b44f-1c02f85dcd0f/data/emory_pt24/spatial_ir/clonotype_output/parameter_set_1
emory_pt26,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt26/spatial_rna,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt26,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt26/spatial_ir/ir_fastq,"SRR15052438_R1.fastq.gz,SRR15052438_R2.fastq.gz",s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/ca89a503-047d-4961-b44f-1c02f85dcd0f/data/emory_pt26/spatial_ir/clonotype_output/parameter_set_1
emory_pt27,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt27/spatial_rna,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/2b05144b-5cbe-4db4-8d83-dcc61aa8b983/data/emory_pt27,s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/4c324020-84b8-474c-9a55-7736554585c4/data/emory_pt27/spatial_ir/ir_fastq,"SRR15052437_R2.fastq.gz,SRR15052437_R1.fastq.gz",s3://project-d57d7407-4c32-4256-bd2e-aa1e037569aa/datasets/ca89a503-047d-4961-b44f-1c02f85dcd0f/data/emory_pt27/spatial_ir/clonotype_output/parameter_set_1
""".strip()


class PreprocessTest(unittest.TestCase):
    """
    Tests for preprocess.py
    """

    def test_formats_samplesheet(self):
        """Test get_sample_paths function"""
        manifest = get_sample_paths(
            samples=pandas.read_csv(StringIO(TEST_SAMPLESHEET)),
            files=pandas.read_csv(StringIO(TEST_FILES)),
            sample_label="sample",
        )
        manifest_csv = manifest.to_csv(index=False).strip()
        self.assertEqual(manifest_csv, TEST_MANIFEST)
