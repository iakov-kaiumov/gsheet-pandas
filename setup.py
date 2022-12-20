from setuptools import setup


with open('requirements.txt') as f:
    required = f.read().splitlines()


setup(
    name='gsheet-pandas',
    version='0.1.0',
    description='Download and upload pandas dataframes to the Google sheets',
    url='https://github.com/iakov-kaiumov/gsheet-pandas',
    author='Iakov Kaiumov',
    author_email='shudson@anl.gov',
    license='MIT License',
    packages=['gsheet-pandas'],
    install_requires=required,
)
