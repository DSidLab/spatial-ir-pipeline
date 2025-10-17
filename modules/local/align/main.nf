process ALIGN_MIXCR {
    tag "$sampleid"
    label "process_low"
    container "ghcr.io/dsidlab/spatial-ir-pipeline:latest"

    input:
    tuple val(sampleid), path(sample_path), val(ir_read_ids), path(ir_fastq_path)
    output:
    tuple val(sampleid), path("${prefix}/$sampleid/spatial_ir/clonotype_output"), emit: aligned_paths
    path "versions.yml", emit: versions

    script:
    prefix = task.ext.prefix ?: "${sampleid}"
    template 'mixcr.py'
}