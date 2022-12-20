from setuptools import setup


setup(
    name='gsheet-pandas',
    version='0.1.1',
    description='Download and upload pandas dataframes to the Google sheets',
    url='https://github.com/iakov-kaiumov/gsheet-pandas',
    author='Iakov Kaiumov',
    author_email='shudson@anl.gov',
    license='MIT License',
    packages=['gsheet-pandas'],
    install_requires=[
        'google-api-python-client',
        'google-api-core',
        'google-auth-oauthlib',
        'pandas',
    ]
)
