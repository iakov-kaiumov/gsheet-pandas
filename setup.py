from setuptools import setup, find_packages
from pathlib import Path


BASE_DIR = Path(__file__).parent
long_description = (BASE_DIR / "readme.md").read_text()

setup(
    name='gsheet-pandas',
    version='0.1.7',
    description='Download and upload pandas dataframes to the Google sheets',
    url='https://github.com/iakov-kaiumov/gsheet-pandas',
    author='Iakov Kaiumov',
    author_email='kaiumov.iag@phystech.edu',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='MIT License',
    packages=find_packages(),
    package_dir={'gsheet-pandas': 'gsheet_pandas'},
    install_requires=[
        'google-api-python-client',
        'google-api-core',
        'google-auth-oauthlib',
        'pandas',
    ]
)
