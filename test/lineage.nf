#!/usr/bin/env nextflow

nextflow.enable.dsl=2

params.registry = 's3://quilt-ernest-staging'
params.input = 'nf-quilt/input'
params.output = 'nf-quilt/output'

process download {
  output:
    stdout

  """
  printf '${params}'
  """
}

workflow {
  download | view { it.trim() }
}
//  download | checksum | transform | upload | package | lineage
