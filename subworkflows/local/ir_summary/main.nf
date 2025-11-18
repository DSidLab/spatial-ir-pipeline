//
// Subworkflow for immune receptor Quarto report generation
//

include { QUARTONOTEBOOK } from "../../../modules/nf-core/quartonotebook/main"
include { MERGE_SDATA    } from "../../../modules/local/merge_sdata"

workflow IR_SUMMARY {
    take:
    ch_samples
    ch_workflow_summary
    ch_collated_versions

    main:

    ch_versions = channel.empty()

    // Quarto report and extensions files
    ir_summary_notebook = file("${projectDir}/bin/ir_summary_report/*")
    extensions = channel.fromPath("${projectDir}/assets/_extensions").collect()
    //
    ch_samples
        .map { sample -> sample[1] }
        .collect()
        .map { sample_paths ->
            tuple([id: "merged"], sample_paths)
        }
        .set { ch_sdata_files }
    //
    MERGE_SDATA(ch_sdata_files)
    ch_versions = ch_versions.mix(MERGE_SDATA.out.versions)
    ch_merged_sdata = MERGE_SDATA.out.sdata
    //
    ch_merged_sdata.map{sdata -> sdata[1]}.set{ch_input_files}
    ch_input_files = ch_input_files.mix(ch_workflow_summary, ch_collated_versions)
    //
    QUARTONOTEBOOK(
        [[id: 'spatial_ir_report'], ir_summary_notebook],
        [input_sdata: "merged_sdata.zarr",version_file: "spatial-ir-pipeline_software_versions.yml",parameters_file: "workflow_summary.yaml"],
        ch_input_files.toList(),
        extensions,
    )

    ch_versions = ch_versions.mix(QUARTONOTEBOOK.out.versions)

    emit:
    merged_sdata = ch_merged_sdata
    report_dir  = QUARTONOTEBOOK.out.report_dir.toList()
    versions     = ch_versions
}
