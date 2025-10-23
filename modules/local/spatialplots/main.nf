process SPATIAL_PLOTS {
    tag "${meta.id}"
    label "process_low"
    container "ghcr.io/dsidlab/spatial-ir-pipeline:latest"

    input:
    tuple val(meta), path(sample_path)

    script:
    prefix = task.ext.prefix ?: "${meta.id}"
    template('spatialplots.py')

    output:
    tuple val(meta.id), val(prefix), emit: output_paths
    path "versions.yml", emit: versions
}
