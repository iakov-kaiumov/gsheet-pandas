from setuptools import setup


setup(
    name='gsheet-pandas',
    version='0.1.2',
    description='Download and upload pandas dataframes to the Google sheets',
    url='https://github.com/iakov-kaiumov/gsheet-pandas',
    author='Iakov Kaiumov',
    author_email='kaiumov.iag@phystech.edu',
    license='MIT License',
    packages=['gsheet_pandas'],
    package_dir={'gsheet_pandas': 'gsheet-pandas'},
    install_requires=[
        'google-api-python-client',
        'google-api-core',
        'google-auth-oauthlib',
        'pandas',
    ]
)
