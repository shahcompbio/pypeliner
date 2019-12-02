from setuptools import setup, find_packages

import versioneer

setup(
    name='pypeliner',
    packages=find_packages(),
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='A library for creating informatic workflows or pipelines.',
    author='Andrew McPherson',
    author_email='andrew.mcpherson@gmail.com',
    url='http://bitbucket.org/dranew/pypeliner',
    download_url='https://bitbucket.org/dranew/pypeliner/get/v{}.tar.gz'.format(versioneer.get_version()),
    keywords=['scientific', 'framework'],
    classifiers=[],
    package_data={'pypeliner': ['tests/*.input']},
    entry_points={'console_scripts': ['pypeliner_delegate=pypeliner.delegator:main',
                                      'aws_fetch_run=pypeliner.contrib.aws.fetch_run:main'],
    },
    install_requires=[
        'dill',
        'networkx',
        'pyyaml',
        'six',
    ],
)
