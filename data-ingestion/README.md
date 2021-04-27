# Data Ingestion
This repository creates an AWS Glue job using the logic in the `/src` directory

## Quickstart
* Set up your [development environment](../development-environment.md)
* Run tests `cd data-ingestion && python -m pytest`
* Deploy: simply push the code, Github Actions will deploy using the workflow for your branch
* [Run the AWS Glue job](https://docs.aws.amazon.com/glue/latest/dg/console-jobs.html)

## Goal of Exercise
Ingest input csv files and output them as parquet to specified locations:
- Make sure that Spark properly uses the csv header and separator 
- Make sure that column names are compatible with Apache Parquet