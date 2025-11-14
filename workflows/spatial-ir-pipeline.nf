/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
include { MULTIQC                } from '../modules/nf-core/multiqc'
include { ALIGN                  } from '../subworkflows/local/align'
include { PREPROCESS_SPATIAL_IR  } from '../modules/local/preprocess'
include { IR_SUMMARY             } from '../subworkflows/local/ir_summary'
include { PREPARE_REPORT_INPUTS  } from '../subworkflows/local/prepare_report_inputs'
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow SPATIAL_IR_PIPELINE {
    take:
    ch_samplesheet // channel: samplesheet read in from --input

    main:

    ch_versions = channel.empty()
    ch_multiqc_files = channel.empty()
    //
    // SUBWORKFLOW: Run ALIGN
    //
    ALIGN(ch_samplesheet.map { sample -> tuple(sample[0], sample[1], sample[5], sample[6], sample[7], sample[8]) })
    //
    ch_multiqc_files = ch_multiqc_files.mix(ALIGN.out.ch_output_from_fastqc)
    ch_versions = ch_versions.mix(ALIGN.out.versions)
    //
    // MODULE: RUN PREPROCESS
    //
    ch_samples = ch_samplesheet.map { sample -> tuple(sample[0], sample[1], sample[2], sample[3]) }
    PREPROCESS_SPATIAL_IR(ch_samples.combine(ALIGN.out.ch_output_samples, by: 0))
    //
    // SUBWORKFLOW: RUN PREPARE_REPORT_INPUTS
    //
    PREPARE_REPORT_INPUTS(ch_versions, ch_multiqc_files)
    //
    // MODULE: RUN MULTIQC
    //
    MULTIQC(
        PREPARE_REPORT_INPUTS.out.multiqc_files.collect(),
        PREPARE_REPORT_INPUTS.out.multiqc_config.toList(),
        PREPARE_REPORT_INPUTS.out.multiqc_custom_config.toList(),
        PREPARE_REPORT_INPUTS.out.multiqc_logo.toList(),
        [],
        [],
    )
    //
    // SUBWORKFLOW: RUN IR_SUMMARY
    //
    IR_SUMMARY(PREPROCESS_SPATIAL_IR.out.output_paths, PREPARE_REPORT_INPUTS.out.workflow_summary, PREPARE_REPORT_INPUTS.out.versions)
    //
    ch_versions = ch_versions.mix(IR_SUMMARY.out.versions)

    emit:
    multiqc_report = MULTIQC.out.report.toList() // channel: /path/to/multiqc_report.html
    ir_report    = IR_SUMMARY.out.report_dir // channel: /path/to/spatial_ir_report
    versions       = ch_versions // channel: [ path(versions.yml) ]
}
