include { ALIGN_MIXCR } from '../../../modules/local/align/main.nf'

workflow ALIGN {
  take:
  ch_samples // samplesheet channel

  main:

  versions = Channel.empty()

  // ---- PREPARE ALIGNMENT branch: select samples to align ----
  ch_samples
    .filter { it[3] }
    .map { tuple(it[0], it[1], it[4], it[5]) }
    .set { align_map }

  // ---- NEW ALIGNMENTS branch: run alignment for requested samples ----
  ALIGN_MIXCR(align_map)
  ALIGN_MIXCR.out.aligned_paths
    .map { meta, run_paths ->
      def csv = run_paths.listFiles().collect { input -> "${input.getParent()}/${input.getName()}" }.join(',')
      return tuple(meta, csv)
    }
    .set { aligned_map }

  // ---- MERGE branch: combine new aligned and original ----

  ch_samples.filter { !it[3] }.map { tuple(it[0], it[2]) }.concat(aligned_map).set { merged }

  versions = versions.mix(ALIGN_MIXCR.out.versions)

  emit:
  merged
  versions
}
