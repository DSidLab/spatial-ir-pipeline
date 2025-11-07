//
// Merge per-sample SpatialData into a single SpatialData
//
process MERGE_SDATA {
    tag "${meta.id}"
    label "process_medium"
    container "ghcr.io/dsidlab/spatial-ir-pipeline:latest"

    input:
    tuple val(meta), path(sdata, stageAs: "?/*")

    when:
    task.ext.when == null || task.ext.when

    script:
    prefix = task.ext.prefix ?: "${meta.id}"
    template("merge_sdata.py")

    output:
    tuple val(meta), file("${prefix}/merged_sdata.zarr"), emit: sdata
    path ("versions.yml"), emit: versions
}
