# CO2
> We present a direct physical measure of the direct climate heating influence of greenhouse gas enhancements, showing how they have increased dramatically since the onset of the industrial revolution. The conclusion that humans are nearly 100% responsible is inescapable. The contributions of different greenhouse gases have evolved over time. We present tangible examples of the excess heat being retained in the Earth system, easily large enough to force global and regional climate change. The direct heating influence of greenhouse gases is well understood. It is explained by the same science that gave us lasers, fluorescent lights, LEDs, cell phones and more. It is thus not surprising that we experience the impact of our emissions on the climate. This heating influence has been reported by NOAA through a range of national and international assessments.

~ [ESRL NOAA (full article)](https://www.esrl.noaa.gov/gmd/ccgg/ghgpower/)

## Goal
Visualise CO2 trend from the Hohenpeissenberg station and calculate linear regression.

## Germany weather stations CO2
* [Schneefernerhaus (at the summit of Zugspitze)](https://schneefernerhaus.de/station/schneefernerhaus/)
* [Hohenpeissenberg, Germany (HPB)](https://www.dwd.de/DE/forschung/atmosphaerenbeob/zusammensetzung_atmosphaere/hohenpeissenberg/start_mohp_node.html)
* Ochsenkopf, Germany (OXK)

## Data Sources
### Earth System Research Laboratories (ESRL)
* [Data Source (ESRL - Search GUI)](https://www.esrl.noaa.gov/gmd/dv/data/index.php?pageID=2&parameter_name=Carbon%2BDioxide&site=HPB&search=Germany&frequency=Discrete)
* Dates covered: 2006-04-06 - 2019-12-30
* [Description of data](https://www.esrl.noaa.gov/gmd/aftp/data/trace_gases/co2/flask/surface/README_surface_flask_co2.html)
* [Data (txt format)](https://www.esrl.noaa.gov/gmd/aftp/data/trace_gases/co2/flask/surface/co2_hpb_surface-flask_1_ccgg_event.txt)
* Other  
    * [Visualisation: All stations](https://www.esrl.noaa.gov/gmd/dv/iadv/)
    * [Visualisation: All stations (trends)](https://www.esrl.noaa.gov/gmd/ccgg/trends/)

### Integrated Carbon Observation System (ICOS)
* [Data Source (ICOS - Search GUI)](https://data.icos-cp.eu/portal/#%7B%22filterCategories%22%3A%7B%22station%22%3A%5B%22iAS_HPB%22%5D%7D%7D)
* Dates covered: 2020-06-01 - current
* Updated daily. Update time = unknown (last known: 2021-04-12 10:06:42)
* [ICOS SparkQL Query (to return 2020-current results)](./icos/sparkql-list-objects.txt)
* View Datasets: `./go-icos datasets`  
* Data: `./go-icos download`
* Other  
    * [DWD one-pager on ICOS](https://www.dwd.de/EN/research/observing_atmosphere/composition_atmosphere/trace_gases/cont_nav/climate_gases_node.html)
    
## Other
* [Comparison of Continuous In-Situ CO2 Measurements with Co-Located Column-Averaged XCO2 TCCON/Satellite Observations and CarbonTracker Model Over the Zugspitze Region](https://mediatum.ub.tum.de/doc/1546480/1546480.pdf)
    * Get inspired! They list their sources and basic methodology
    * [Carbon Tracker](https://www.carbontracker.eu/overview.shtml)
* DWD is the current authority of Climate stuff in Germany
    * [List of data sources](https://www.dwd.de/DE/klimaumwelt/cdc/klinfo_systeme/klinfo_systeme.html?nn=17626) - but does not have CO2 data
    * [Germany Climate Data Center (DWD)](https://www.dwd.de/EN/climate_environment/cdc/cdc_node_en.html;jsessionid=0E6B711A787AC89AB80D489850857627.live11052)
    * [Station Data for HPB](https://www.dwd.de/DE/leistungen/klimadatendeutschland/klimadatendeutschland.html?view=renderJsonResults&undefined=Absenden&cl2Categories_LeistungsId=klimadatendeutschland&lsId=343278&cl2Categories_Station=klimadatendeutschland_hohenpeissenberg&cl2Categories_ZeitlicheAufloesung=klimadatendeutschland_tageswerte&cl2Categories_Format=text) with [schema](https://www.dwd.de/DE/leistungen/klimadatendeutschland/beschreibung_tagesmonatswerte.html?nn=16102&lsbId=343278) - but NOT CO2 data
    * [HPB CO2 visualisations (no raw data)](CO2 Graph (no data) https://www.dwd.de/EN/research/observing_atmosphere/composition_atmosphere/hohenpeissenberg/img/default_co2.html?nn=489982)
* World Meterological Organisation (WMO)
    * They have an [API](https://worldweather.wmo.int/en/dataguide.html) and a JSON schema
    * No CO2
* [Global Atmopheric Watch](https://www.dwd.de/EN/research/observing_atmosphere/composition_atmosphere/hohenpeissenberg/cont_nav/gaw.html)