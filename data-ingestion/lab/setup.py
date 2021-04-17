from setuptools import setup, find_packages
setup(
    name="twdu_ingestion",
    version="0.1",
    # packages=find_packages()
    packages=["twdu_ingestion"] #https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html#create-python-extra-library
)