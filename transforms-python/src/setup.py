#!/usr/bin/env python
"""Python project setup script."""

import os
from setuptools import find_packages, setup

setup(
    name=os.environ['PKG_NAME'],
    version=os.environ['PKG_VERSION'],

    description='Python data transformation project',

    # Modify the author for this project
    author="NHS",

    packages=find_packages(exclude=['contrib', 'docs', 'test']),

    # Please instead specify your dependencies in conda_recipe/meta.yml
    install_requires=[],

    entry_points={
        'transforms.pipelines': [
            'root = myproject.pipeline:my_pipeline'
        ]
    }
)
