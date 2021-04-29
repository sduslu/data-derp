# Data Transformation
This repository creates an AWS Glue job using the logic in the `/src` directory

## Quickstart
* Set up your [development environment](../development-environment.md)
* Run tests `cd data-transformation` then `python -m pytest`
* Deploy: simply push the code, Github Actions will deploy using the workflow for your branch
* [Run the AWS Glue job](https://docs.aws.amazon.com/glue/latest/dg/console-jobs.html)

## Goal of Exercise
Transform the ingested files that resulted from the AWS Glue Job in `../data-ingestion`. Don't forget your tests!
