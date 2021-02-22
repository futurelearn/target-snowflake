#!/usr/bin/env python
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name="target-snowflake",
    version="0.1.2",
    author="Meltano",
    author_email="meltano@gitlab.com",
    description="Singer.io target for importing data to Snowflake",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/meltano/target-snowflake",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=[
        'inflection==0.3.1',
        'singer-python==5.2.3',
        'asn1crypto<1.0.0',
        # conflicts resolution, see https://github.com/snowflakedb/snowflake-connector-python/issues/225
        'azure-storage-blob<12.0',
        'snowflake-connector-python==1.7.11',
        'snowflake-sqlalchemy==1.1.12',
        # conflicts resolution, see https://gitlab.com/meltano/meltano/issues/193
        'idna==2.7',
    ],
    extras_require={
        'dev': [
            'pytest>=3.8',
            'black>=18.3a0',
            'freezegun'
        ]
    },
    entry_points="""
    [console_scripts]
    target-snowflake=target_snowflake:main
    """,
    python_requires='>=3.6',
    packages=find_packages(exclude=['tests']),
    package_data={},
    include_package_data=True,
)
