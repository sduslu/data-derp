# Data Transformation
This repository creates an AWS Glue job using the logic in the `/src` directory

## Quickstart
* Set up your [development environment](../development-environment.md)
* Run tests `cd data-transformation/src && python -m pytest`
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

## Notes
### File Structure
In an ideal world, you might use an `src/` and `test/` directory to intentionally package your production code and keep test files separate. It is also one of the [sensible defaults by Pytest](https://docs.pytest.org/en/reorganize-docs/new-docs/user/directory_structure.html). We realised that it would be a disaster for this multi-python-project repository (and the dev experience) because it would require different PYTHONPATH to be set for every subproject. As a result, the tests are inline with the code in `src/`. You can then run tests by pressing the green PLAY button or running `python -m pytest`. But in general, always follow those sensible defaults.