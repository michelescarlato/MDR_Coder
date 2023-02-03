# MDR_Coder;
Transfers session data into accumulated data tables.

The program takes the data in the session data or sd tables in each source database (the 'session data' being created by the most recent harvest operation), and compares it with the accumulated data for each source, which is stored in the accumulated data (ad) tables. New and revised data are then transferred to the ad tables.<br/>
The program represents the third stage in the 4 stage MDR extraction process:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Download => Harvest => **Import** => Aggregation<br/><br/>
For a much more detailed explanation of the extraction process,and the MDR system as a whole, please see the project wiki (landing page at https://ecrin-mdr.online/index.php/Project_Overview).<br/>
In particular, for the Import process, please see:<br/>
https://ecrin-mdr.online/index.php/Importing_Data and <br/>
http://ecrin-mdr.online/index.php/Missing_PIDs_and_Hashing<br/>
and linked pages.

### Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087
