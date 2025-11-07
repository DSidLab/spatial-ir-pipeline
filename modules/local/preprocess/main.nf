process PREPROCESS_SPATIAL_IR {
    tag "${meta.id}"
    label "process_low"
    container "ghcr.io/dsidlab/spatial-ir-pipeline:latest"

    input:
    tuple val(meta), path(sample_path), path(spatial_rna), path(cell_annotations), val(clonotype_output)

    script:
    prefix = task.ext.prefix ?: "${meta.id}"
    template('preprocess.py')

    output:
    tuple val(meta), path("${prefix}/sdata_pp.zarr"), path("${prefix}/adata_raw.h5ad"), emit: output_paths
    path "versions.yml", emit: versions
}
