# nf-quilt3

Nextflow plugin for interacting with [Quilt](https://quiltdata.com/) packages as a FileSystem

`nf-quilt3` enables you read and write directly to Quilt packages using `quilt` URLs
wherever you currently use `s3`, `az` or `gs` URLs.


## Getting Started

To add the `nf-quilt3` plugin to your workflow, you need Nextflow 22.09 (or later) and Python 3.9 (or later).

### Pipeline Configuration

Add the following snippet to your `nextflow.config` to enable the plugin:
```groovy
plugins {
    id 'nf-quilt3'
}
```

### Quilt Configuration

This plugin uses [Java Embedded Python](https://github.com/ninia/jep) to call the Quilt API.
To use it, your container must install and link to the appropriate libraries:

```bash
pip3 install quilt3 jep # -r requirements.txt
export DYLD_LIBRARY_PATH=`pip3 show jep|awk '/^Location/ {print $2}'` # macOS
export LD_LIBRARY_PATH=`pip3 show jep|awk '/^Location/ {print $2}'` # Linux
# Do not overwrite if already set: if [ ${1+x} ]; then https://stackoverflow.com/questions/3601515/how-to-check-if-a-variable-is-set-in-bash
```

### Reading and Writing Quilt URLs

Next, create a Quilt URL for the S3 bucket where you want to store (and eventually read) your results.
You must also specify a package name containing exactly one '/', such as `instrument/experiment`
Finally, run your Nextflow pipeline with your config file, setting that URL as your output directory, .e.g.:

```
nextflow run my/ingest --outdir quilt://raw-bucket/instrument/experiment
```

You can then use that package as input to future jobs, e.g.:

```
nextflow run my/analysis --indir quilt://raw-bucket/instrument/experiment --outdir quilt://prod-bucket/analysis/experiment
```

## Additional Features



# Development

_Based on [nf-hello](https://github.com/nextflow-io/nf-hello)_

## Unit testing

Run the following command in the project root directory (ie. where the file `settings.gradle` is located):

```bash
make check
```

## Testing and debugging

1. Clone the Nextflow repository into a sibling directory, .e.g:

    ```bash
    git clone --depth 1 https://github.com/nextflow-io/nextflow ../nextflow
    ```

2. Compile the plugin alongside the Nextflow code:
    ```bash
    make compile
    ```

3. Run Nextflow with the plugin, using `./launch.sh` as a drop-in replacement for the `nextflow` command, and adding the option `-plugins nf-quilt3` to load the plugin:
    ```bash
    ./launch.sh run nextflow-io/hello -plugins nf-quilt3
    ```

## Package, upload and publish

The project should be hosted in a GitHub repository whose name should match the name of the plugin, that is the name of the directory in the `plugins` folder (e.g. `nf-hello`).

Follow these steps to package, upload and publish the plugin:

1. Create a file named `gradle.properties` in the project root containing the following attributes (this file should not be committed to Git):

   * `github_organization`: the GitHub organisation where the plugin repository is hosted.
   * `github_username`: The GitHub username granting access to the plugin repository.
   * `github_access_token`: The GitHub access token required to upload and commit changes to the plugin repository.
   * `github_commit_email`: The email address associated with your GitHub account.

2. Use the following command to package and create a release for your plugin on GitHub:
    ```bash
    ./gradlew :plugins:nf-hello:upload
    ```

3. Create a pull request against [nextflow-io/plugins](https://github.com/nextflow-io/plugins/blob/main/plugins.json) to make the plugin accessible to Nextflow.
