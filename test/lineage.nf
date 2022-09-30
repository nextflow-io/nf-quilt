#!/usr/bin/env nextflow

nextflow.enable.dsl=2

params.registry = 's3://quilt-ernest-staging'
params.input = 'nf-quilt/input'
params.output = 'nf-quilt/output'

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
    path 'input/*'

  """
  #!/usr/bin/env python3
  import quilt3
  quilt3.Package().install('${params.input}', registry='${params.registry}', dest='input', top_hash='$hash')
  """
}

workflow {
  get_hash | download | view // { it.trim() }
}
//  download | checksum | transform | upload | package | lineage
