# Copyright (C) Seqera Labs S.L.- All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited
# Proprietary and confidential
# Written by Ben Sherman <bentshermann@gmail.com>

from setuptools import setup, find_packages

setup(
    name='quilt-cli',
    version='0.1.0',
    packages=find_packages(),
    python_requires='>=3.7',
    author='seqeralabs',
    url='https://github.com/nextflow-io/quilt-cli',
    install_requires=[
        'awscli',
        'quilt3',
    ],
    entry_points={
        'console_scripts': ['quilt-cli = quilt_cli.__main__:main'],
    }
)
