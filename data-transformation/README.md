* 1 S3 bucket per user
    * /sink (dir) (where we put transformed data)
        * co2-temp-country
        * co2-temp-global
        * ...
* Glue job for transformation
  * params
    * config
      * berkley-temperpature-global 
      * berkley-temperature-country
      * owid-co2-country
  * [output](#output)
  * Python
  * reading url for datasets from config
  * saving as parquet  
  * tradeoffs conversation
    * how much pre-aggregation
    
## Output
| year | country | CO2 (ppm) | country temp (C) | global temp (C) |
| --- | --- | --- | --- | --- |
| 2020 | Germany | 190 | 14 | 21 |
| 2020 | France | 188 | 13 | 21 |
| 2021 | Germany | 200 | 16 | 22 |
| 2021 | France | 205 | 15 | 22 |
