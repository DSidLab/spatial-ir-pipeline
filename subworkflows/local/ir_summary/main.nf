//
// Subworkflow for immune receptor Quarto report generation
//

include { QUARTONOTEBOOK } from "../../../modules/nf-core/quartonotebook/main"
include { MERGE_SDATA    } from "../../../modules/local/merge_sdata"

workflow IR_SUMMARY {
    take:
    ch_samples

    main:

    ch_versions = channel.empty()

    // Quarto report and extensions files
    ir_summary_notebook = file("${projectDir}/bin/ir_summary.qmd", checkIfExists: true)
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
    QUARTONOTEBOOK(
        [[id: 'ir_summary_report'], ir_summary_notebook],
        [input_sdata: "merged_sdata.zarr"],
        ch_merged_sdata.map { sdata -> sdata[1] },
        extensions,
    )

    ch_versions = ch_versions.mix(QUARTONOTEBOOK.out.versions)

    emit:
    merged_sdata = ch_merged_sdata
    versions     = ch_versions
}
