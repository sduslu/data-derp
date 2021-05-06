# Data Transformation
This repository creates an AWS Glue job using the logic in the `/src` directory

## Quickstart
* Set up your [development environment](../development-environment.md)
* Run tests `cd data-transformation` then `python -m pytest` (Fix the tests!)
* Deploy: simply push the code, Github Actions will deploy using the workflow for your branch
* [Run the AWS Glue job](https://docs.aws.amazon.com/glue/latest/dg/console-jobs.html)

## View the Spark UI
[Install the latest version of the SessionsManager CLI](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html).
From the root of the repository:
```
./data-derp bootstrap switch-role -p <project-name> -m <module-name>
./data-derp aws-spark-ui -p <project-name> -m <module-name>
```
Navigate to http://localhost:18080

## Goal of Exercise
Transform the ingested files that resulted from the AWS Glue Job in `../data-ingestion`. Don't forget your tests!
