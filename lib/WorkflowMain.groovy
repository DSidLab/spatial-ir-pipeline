//
// This file holds several functions specific to the main.nf workflow in the btc/spatial pipeline
//

import nextflow.Nextflow

class WorkflowMain {

    //
    // Citation string for pipeline
    //
    public static String citation(workflow) {
        return "If you use ${workflow.manifest.name} for your analysis please cite:\n\n" +
            "* The pipeline\n" +
            "  https://github.com/dsidlab/spatial-ir-pipeline" +
            "* The nf-core framework\n" +
            "  https://doi.org/10.1038/s41587-020-0439-x\n\n" +
            "* Software dependencies\n" +
            "  ${workflow.manifest.homePage}/blob/main/CITATIONS.md"
    }


    //
    // Validate parameters and print summary to screen
    //
    public static void initialise(workflow, params, log) {
        
        // Print workflow version and exit on --version
        if (params.version) {
            def logo = NfcoreTemplate.logo(workflow, params.monochrome_logs)
            def citation = '\n' + WorkflowMain.citation(workflow) + '\n'
            String workflow_version = "\n ${workflow.manifest.name} - version: ${NfcoreTemplate.version(workflow)} \n"
            String dashed_line = NfcoreTemplate.dashedLine(params.monochrome_logs)
            log.info  logo + citation + dashed_line + workflow_version + dashed_line
            System.exit(0)
        }

        // Check that a -profile or Nextflow config has been provided to run the pipeline
        NfcoreTemplate.checkConfigProvided(workflow, log)

        // Check that conda channels are set-up correctly
        if (workflow.profile.tokenize(',').intersect(['conda', 'mamba']).size() >= 1) {
            Utils.checkCondaChannels(log)
        }

        // Check AWS batch settings
        NfcoreTemplate.awsBatch(workflow, params)

    }
}
