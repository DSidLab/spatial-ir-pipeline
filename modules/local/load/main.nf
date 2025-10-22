process LOAD_SPATIAL_IR {
    tag "${meta.id}"
    label "process_low"
    container "ghcr.io/dsidlab/spatial-ir-pipeline:latest"

    input:
    tuple val(meta), path(sample_path), path(spatial_rna), val(clonotype_output)

    script:
    prefix = task.ext.prefix ?: "${meta.id}"
    template('load_visium.py')

    output:
    tuple val(meta), path("${prefix}/adata.h5ad"), emit: output_paths
    path "versions.yml", emit: versions
}
