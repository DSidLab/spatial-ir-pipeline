include { ALIGN_MIXCR } from '../../../modules/local/align/main.nf'

workflow ALIGN {
  take:
  ch_samples // samplesheet channel

  main:

  versions = Channel.empty()

  // ---- PREPARE ALIGNMENT branch: select samples to align ----
  ch_samples
    .filter { it[6] }
    .map { tuple(it[0], it[1], it[7], it[8]) }
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
  ch_samples
    .cross(aligned_map)
    .map { origs, new_cln_dirs ->
      def (meta, sample_path, c2, c3, c4, cln_dirs, align, ir_read_ids, ir_fastq_path) = origs
      def out_cln_dirs = new_cln_dirs[1] ? new_cln_dirs[1] : cln_dirs
      tuple(meta, sample_path, c2, c3, c4, out_cln_dirs, align, ir_read_ids, ir_fastq_path)
    }
    .set { crossed_samples }

  ch_samples.filter { !it[6] }.concat(crossed_samples).set { merged }

  versions = versions.mix(ALIGN_MIXCR.out.versions)

  emit:
  merged
  versions
}
