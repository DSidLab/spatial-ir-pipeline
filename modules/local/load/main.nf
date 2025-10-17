process LOAD_SPATIAL_IR {
    tag "$sampleid"
    label "process_low"
    container "ghcr.io/dsidlab/spatial-ir-pipeline:latest"

    input:
    tuple val(sampleid), path(sample_path)
    output:
    tuple val(sampleid), path("${prefix}/$sampleid/spatial_ir"), emit: output_paths
    path "versions.yml", emit: versions

    script:
    prefix = task.ext.prefix ?: "${sampleid}"
    template 'diversity.py'
}