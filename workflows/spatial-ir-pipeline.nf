/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
include { MULTIQC                } from '../modules/nf-core/multiqc'
include { paramsSummaryMap       } from 'plugin/nf-schema'
include { paramsSummaryMultiqc   } from '../subworkflows/nf-core/utils_nfcore_pipeline'
include { softwareVersionsToYAML } from '../subworkflows/nf-core/utils_nfcore_pipeline'
include { methodsDescriptionText } from '../subworkflows/local/utils_nfcore_spatial-ir-pipeline_pipeline'
include { ALIGN                  } from '../subworkflows/local/align'
include { PREPROCESS_SPATIAL_IR  } from '../modules/local/preprocess'
include { IR_SUMMARY             } from '../subworkflows/local/ir_summary'

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
    ch_multiqc_files = ch_multiqc_files.mix(ALIGN.out.ch_fastqc)
    ch_versions = ch_versions.mix(ALIGN.out.versions)
    //
    // MODULE: RUN PREPROCESS
    //
    ch_samples = ch_samplesheet.map { sample -> tuple(sample[0], sample[1], sample[2], sample[3]) }
    PREPROCESS_SPATIAL_IR(ch_samples.combine(ALIGN.out.merged, by: 0))
    //
    // SUBWORKFLOW: RUN IR_SUMMARY
    //
    IR_SUMMARY(PREPROCESS_SPATIAL_IR.out.output_paths)
    //
    ch_versions = ch_versions.mix(IR_SUMMARY.out.versions)
    //
    // Collate and save software versions
    //
    softwareVersionsToYAML(ch_versions)
        .collectFile(
            storeDir: "${params.outdir}/pipeline_info",
            name: 'spatial-ir-pipeline_software_mqc_versions.yml',
            sort: true,
            newLine: true,
        )
        .set { ch_collated_versions }
    //
    // MODULE: MultiQC
    //
    ch_multiqc_config = channel.fromPath(
        "${projectDir}/assets/multiqc_config.yml",
        checkIfExists: true
    )
    ch_multiqc_custom_config = params.multiqc_config
        ? channel.fromPath(params.multiqc_config, checkIfExists: true)
        : channel.empty()
    ch_multiqc_logo = params.multiqc_logo
        ? channel.fromPath(params.multiqc_logo, checkIfExists: true)
        : channel.empty()
    summary_params = paramsSummaryMap(
        workflow,
        parameters_schema: "nextflow_schema.json"
    )
    ch_workflow_summary = channel.value(paramsSummaryMultiqc(summary_params))
    ch_multiqc_files = ch_multiqc_files.mix(
        ch_workflow_summary.collectFile(name: 'workflow_summary_mqc.yaml')
    )
    ch_multiqc_custom_methods_description = params.multiqc_methods_description
        ? file(params.multiqc_methods_description, checkIfExists: true)
        : file("${projectDir}/assets/methods_description_template.yml", checkIfExists: true)
    ch_methods_description = channel.value(
        methodsDescriptionText(ch_multiqc_custom_methods_description)
    )

    ch_multiqc_files = ch_multiqc_files.mix(ch_collated_versions)
    ch_multiqc_files = ch_multiqc_files.mix(
        ch_methods_description.collectFile(
            name: 'methods_description_mqc.yaml',
            sort: true,
        )
    )

    MULTIQC(
        ch_multiqc_files.collect(),
        ch_multiqc_config.toList(),
        ch_multiqc_custom_config.toList(),
        ch_multiqc_logo.toList(),
        [],
        [],
    )

    emit:
    multiqc_report = MULTIQC.out.report.toList() // channel: /path/to/multiqc_report.html
    versions       = ch_versions // channel: [ path(versions.yml) ]
}
