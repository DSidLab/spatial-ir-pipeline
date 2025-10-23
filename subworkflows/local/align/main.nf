include { ALIGN_MIXCR } from '../../../modules/local/align/main.nf'
include { FASTQC      } from '../../../modules/nf-core/fastqc/main.nf'

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

  // ---- RUN FASTQC on new alignments ----
  align_map
    .map { tuple(it[0], it[2], it[3]) }
    .map { meta, names, base ->
      def parts = names.split(/\s*,\s*/).findAll { it }
      def f1 = (base as Path).resolve(parts[0])
      def f2 = (base as Path).resolve(parts[1])
      tuple(meta + [single_end: false], [f1, f2])
    }
    .set { ch_fastqs }
  //
  FASTQC(ch_fastqs)
  ch_fastqc = FASTQC.out.zip.collect { it[1] }
  versions = versions.mix(FASTQC.out.versions)

  // ---- MERGE branch: combine new aligned and original ----
  ch_samples.filter { !it[3] }.map { tuple(it[0], it[2]) }.concat(aligned_map).set { merged }

  versions = versions.mix(ALIGN_MIXCR.out.versions)

  emit:
  merged
  ch_fastqc
  versions
}
