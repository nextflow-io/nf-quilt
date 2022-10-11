#!/usr/bin/env nextflow
nextflow.enable.dsl=2

params.src = '/Users/quilt/Downloads/Packages/igv_demo'
params.pub = '/Users/quilt/Downloads/Packages/test_nf22'

pkg_files = Channel.fromPath(params.src+'/*')

process publish {
    publishDir params.pub, mode: 'copy', overwrite: true

    input:
      path x

    output:
      path params.out + '/*'

    """
    mkdir -p output
    cp -r $x output
    echo output/$x
    """
}

workflow {
  pkg_files | publish | view { it }
}