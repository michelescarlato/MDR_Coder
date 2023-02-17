Codes MDR data, where possible, using controlled vocabularies.

The program takes the data in the accumulated data (ad) tables and wherever possible codes it against controlled vocabularies. <br/>
This includes coding organisation names against ROR Ids, for example in the study / object contributor tables and in the data objects table. It also includes providing Geonames codes for country and city data, MeSH codes for topics data, for those not alerady MeSH coded, and it tries to provide ICD code for 'condition under study' data. <br/>
In general coding is obly done for data in the ad schema that has not already been coded - i.e. has been recently imported. It is possible, however, to overwrite this default process and code all the data inthe ad tables, if required. Data that is not coded, i.e. has no equivalent in the controlled vocabularies, is stored for later inspection to see if it can be manually mapped to a CV term.<br/>
The program represents the fourth stage in the 5 stage MDR extraction process:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Download => Harvest => Import => **Coding** => Aggregation<br/><br/>
For a much more detailed explanation of the extraction process,and the MDR system as a whole, please see the project wiki, <br/>
(landing page at https://ecrin-mdr.online/index.php/Project_Overview).<br/>
In particular, for the Coding process, please see:<br/>
https://ecrin-mdr.online/index.php/Coding_Data 
and linked pages.


## Parameters and Usage
The system can take the following parameters:<br/>
**-s:** expects to be followed by a comma separated list of MDR source integer ids, each representing a data source within the system. Data for that source is then coded.<br/>
**-A:** as a flag. If present, forces the (re)coding of all the codable data in the specified sources. This is used if the coding porocess itself has changed or the volume of CV terms available for coding has been substantially augmented.<br/>
Routine usage, as in the scheduled extraction process, is to use -s followed by a list of one or more source ids.<br/>

## Dependencies
The program used the Nuget packages:
* CommandLineParser - to carry out initial processing of the CLI arguments
* Npgsql, Dapper and Dapper.contrib to handle database connectivity
* Microsoft.Extensions.Configuration, .Configuration.Json, and .Hosting to read the json settings file and support the initial host setup.

## Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087
