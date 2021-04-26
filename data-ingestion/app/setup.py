from setuptools import setup, find_packages
setup(
    name="data_ingestion",
    version="0.1",
    packages=find_packages(where='src') #https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html#create-python-extra-library
)