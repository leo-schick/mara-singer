from setuptools import setup, find_packages
import re

def get_long_description():
    with open('README.md') as f:
        return re.sub('!\[(.*?)\]\(docs/(.*?)\)', r'![\1](https://github.com/hz-lschick/mara-singer/raw/master/docs/\2)', f.read())

setup(
    name='mara-singer',
    version='0.7.1',

    description='Singer implementation for mara',

    long_description=get_long_description(),
    long_description_content_type='text/markdown',

    url = 'https://github.com/hz-lschick/mara-singer',

    install_requires=[
        'singer-python==5.9.0',
        'mara-db>=4.7.0',
        'mara-page>=1.5.1',
        'mara-pipelines>=3.0.0'],

    extras_require={
        'test': ['pytest', 'pytest_click', 'mara-app>=2.2.0'],
    },

    packages=find_packages(),

    author='Mara contributors',
    license='MIT',

    entry_points={},
    python_requires='>=3.6'
)
