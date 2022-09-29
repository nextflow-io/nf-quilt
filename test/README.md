# Testing NF-quilt

This minimal test pipeline (inspired by nf-hello) is designed
to test an end-to-end workflow using Quilt Packages and URIs,
demonstrating how data lineage enables a rigorous chain of custody.

## Objectives

The key steps are:

1. Pulling input from a Quilt package
2. "Pinning" the input version to a specific hash
3. Performing a simple transformation (e.g. upper-casing the filenames)
4. Writing the files to S3
5. Converting the files into a new (updated) Quilt package
6. Recording the resulting URI
7. Storing all of that in the Package Metadata
