# nf-quilt

Nextflow plugin for interacting with [Quilt](https://quiltdata.com/) packages.

`nf-quilt` currently allows you to publish the outputs of a workflow run as a Quilt package. WHen you launch a pipeline with the `nf-quilt` plugin, it will publish a Quilt package upon workflow completion that contains output files published to S3.

## Getting Started

To add the `nf-quilt` plugin to your workflow, you need Nextflow 22.04 (or later) and Python 3.7 (or later).

Install the [quilt-cli](./quilt-cli) Python package:
```bash
pip3 install git+https://github.com/nextflow-io/nf-quilt.git#subdirectory=quilt-cli
```

Add the following snippet to your `nextflow.config` to enable the plugin:
```groovy
plugins {
    id 'nf-quilt'
}
```

Configure the plugin with the `quilt` config scope in your `nextflow.config`. At a minimum, you should specify the package name and registry. You can also specify a list of paths to include in the Quilt package; by default, the plugin will include all output files that were published to S3.

_TIP: It is recommended that you use `publishDir` to select outputs for the Quilt package, rather than `quilt.paths`, so that the Quilt package matches the actual workflow outputs._

Here's an example based on `nf-core/rnaseq`:
```groovy
quilt {
  packageName = 'genomes/yeast'
  registry = 's3://seqera-quilt'
  message = 'My commit message'
  meta = [pipeline: 'nf-core/rnaseq']
  force = false
}
```

Finally, run your Nextflow pipeline with your config file. You do not need to modify your pipeline script in order to use the `nf-quilt` plugin. As long as your pipeline publishes the desired output files to S3, the plugin will automatically publish a Quilt package based on your configuration settings.

## Reference

The plugin exposes a new `quilt` config scope which supports the following options:

| Config option 	  | Description 	          |
|---	              |---	                      |
| `quilt.packageName` | Name of package, in the USER/PKG format
| `quilt.registry`    | Registry where to create the new package
| `quilt.message`     | The commit message for the new package
| `quilt.meta`        | Package-level metadata in the form of key-value pairs
| `quilt.force`       | Skip the parent top hash check and create a new revision even if your local state is behind the remote registry
| `quilt.paths`       | List of published files (can be path or glob) to include in the package

## Development

Refer to the [nf-hello](https://github.com/nextflow-io/nf-hello) README for instructions on how to build, test, and publish Nextflow plugins.
