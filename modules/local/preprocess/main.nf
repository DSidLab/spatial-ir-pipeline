process PREPROCESS_SPATIAL_IR {
    tag "${meta.id}"
    label "process_low"
    container "ghcr.io/dsidlab/spatial-ir-pipeline:latest"

    input:
    tuple val(meta), path(sample_path), path(spatial_rna), path(raw_adata_path)

    script:
    prefix = task.ext.prefix ?: "${meta.id}"
    template('preprocess.py')

    output:
    tuple val(meta), path("${prefix}/adata_pp.h5ad"), emit: output_paths
    path "versions.yml", emit: versions
}
