* 1 S3 bucket per user
    * /config (dir)
        * berkley-temperpature-global.txt
        * berkley-temperature-country.txt
        * owid-co2-country.txt
    * /ingested (dir) (where we ingest to)
        * berkley-temperpature-global
        * berkley-temperature-country
        * owid-co2-country
* Glue job for ingestion
  * params
    * config
      * berkley-temperpature-global 
      * berkley-temperature-country
      * owid-co2-country
  * Python
  * reading url from config
  * saving as parquet  