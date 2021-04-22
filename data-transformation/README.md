# Data Transformation
This repository creates an AWS Glue job using the logic in the `/src` directory

## Quickstart
* Set up your [development environment](../development-environment.md)
* Run tests `cd data-transformation/src && pytest`
* Deploy: simply push the code, Github Actions will deploy using the workflow for your branch
* [Run the AWS Glue job](https://docs.aws.amazon.com/glue/latest/dg/console-jobs.html)

## Goal of Exercise
Transform the ingested files that resulted from the AWS Glue Job in `../data-ingestion`. Don't forget your tests!

### Expected Example Output
Process the ingested files
| year | country | CO2 (ppm) | country temp (C) | global temp (C) |
| --- | --- | --- | --- | --- |
| 2020 | Germany | 190 | 14 | 21 |
| 2020 | France | 188 | 13 | 21 |
| 2021 | Germany | 200 | 16 | 22 |
| 2021 | France | 205 | 15 | 22 |