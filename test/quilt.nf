#!/usr/bin/env nextflow
nextflow.enable.dsl=2

params.src = 'quilt3://quilt-example/examples/hurdat'
params.pub = 'quilt3://quilt-ernest-staging/test/hurdat'
//params.src = '/Users/quilt/Downloads/Packages/ernie_igv_demo_ecdcd41'
//params.pub = '/Users/quilt/Downloads/Packages/sprint_2022-09-12'
params.out = 'output'

pkg_files = Channel.fromPath(params.src+'/*')

process printout {
    publishDir params.pub, mode: 'copy', overwrite: true

    input:
      path x

    output:
      path params.out + '/*'

    """
    mkdir -p $params.out
    cp -r $x $params.out
    echo $params.out/$x
    """
}

workflow {
  pkg_files | printout | view { it }
}
