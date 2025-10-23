process ALIGN_MIXCR {
    tag "${meta.id}"
    label "process_medium"
    publishDir "${params.outdir}", mode: params.publish_dir_mode, pattern: "${prefix}/clonotype_output", enabled: true
    container "ghcr.io/dsidlab/spatial-ir-pipeline:latest"

    secret 'MI_LICENSE'

    input:
    tuple val(meta), path(sample_path), val(ir_read_ids), val(ir_fastq_path)

    script:
    prefix = task.ext.prefix ?: "${meta.id}"
    println(task.cpus)
    template("mixcr.py")

    output:
    tuple val(meta), file("${prefix}/clonotype_output"), emit: aligned_paths
    path "versions.yml", emit: versions
}
