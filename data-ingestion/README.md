# Data Ingestion
This repository creates an AWS Glue job using the logic in the `/src` directory

## Quickstart
* Set up your [development environment](../development-environment.md)
* Run tests `cd data-ingestion/src && pytest`
* Deploy: simply push the code, Github Actions will deploy using the workflow for your branch
* [Run the AWS Glue job](https://docs.aws.amazon.com/glue/latest/dg/console-jobs.html)

## Goal of Exercise
Ingest files (input) and output to specified locations (without transformations):

| Input | Output |
| --- | --- |
| [Temperature](https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/Berkley%20Land-Ocean%20Temperature%20Anomaly/Berkley%20Land-Ocean%20Temperature%20Anomaly.csv) | s3://twdu-germany-<workflow-name>/data-ingestion/output/temperature.parquet |
| [CO2](https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/CO2%20emissions%20(Aggregate%20dataset%20(2021))/CO2%20emissions%20(Aggregate%20dataset%20(2021)).csv) | s3://twdu-germany-<workflow-name>/data-ingestion/output/co2.parquet |