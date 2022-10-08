#!/usr/bin/env nextflow

nextflow.enable.dsl=2

params.registry = 's3://quilt-example'
params.input = 'examples/hurdat'
params.indir = 'input'
params.outdir = 'output'
params.pubdir = '/tmp/nf-quilt'

process get_hash {
  output:
    stdout

  """
  #!/usr/bin/env python3
  import quilt3
  p = quilt3.Package().browse('${params.input}', registry='${params.registry}')
  print(p.top_hash,end="")
  """
}

process download {
  input:
    val hash

  output:
    path params.indir

  """
  #!/usr/bin/env python3
  import quilt3
  quilt3.Package().install('${params.input}', registry='${params.registry}', dest='input', top_hash='${hash}')
  """
}

process pub {
  publishDir params.pubdir, mode: 'copy', overwrite: true

  input:
    file params.indir

  output:
    path params.outdir + '/*'

  """
  echo $params.indir
  cp -r $params.indir $params.outdir
  """
}


workflow {
  get_hash | download | flatten | pub | view // { it.trim() }
}
//   get_hash | download | flatten | transform  | pkg | view // { it.trim() }

//  download | checksum | transform | upload | package | lineage
