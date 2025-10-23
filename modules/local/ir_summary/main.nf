process IR_SUMMARY {
    tag "${meta.id}"
    label "process_low"
    container "ghcr.io/dsidlab/spatial-ir-pipeline:latest"

    input:
    tuple val(meta), path(sample_path), path(spatial_rna), path(adata_pp), path(ir_diversity_data)

    script:
    prefix = task.ext.prefix ?: "${meta.id}"
    template('ir_summary.py')

    output:
    path "${prefix}/outs/*report.csv", emit: report
    path "versions.yml", emit: versions
}
