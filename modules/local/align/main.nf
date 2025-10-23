process ALIGN_MIXCR {
    tag "${meta.id}"
    label "process_medium"
    container "ghcr.io/dsidlab/spatial-ir-pipeline:latest"

    secret 'MI_LICENSE'

    input:
    tuple val(meta), path(sample_path), val(ir_read_ids), val(ir_fastq_path)

    script:
    prefix = task.ext.prefix ?: "${meta.id}"
    template("mixcr.py")

    output:
    tuple val(meta), file("${prefix}/clonotype_output"), emit: aligned_paths
    path "versions.yml", emit: versions
}
