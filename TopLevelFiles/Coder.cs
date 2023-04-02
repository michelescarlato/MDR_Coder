namespace MDR_Coder;

public class Coder
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly IMonDataLayer _monDataLayer;

    public Coder(IMonDataLayer monDataLayer, ILoggingHelper loggingHelper)
    {
        _monDataLayer = monDataLayer;
        _loggingHelper = loggingHelper;
    }
    
    public void Run(Options opts)
    {
        try
        {
            // Simply code the data for each listed source.

            foreach (int sourceId in opts.SourceIds!)
            {
                Source? source = _monDataLayer.FetchSourceParameters(sourceId);
                if (source is not null)
                {
                    _loggingHelper.OpenLogFile(source.database_name!);
                    _loggingHelper.LogHeader("STARTING CODER");
                    _loggingHelper.LogCommandLineParameters(opts);
                    CodeData(source, opts);
                }
            }

            _loggingHelper.CloseLog();
        }

        catch (Exception e)
        {
            _loggingHelper.LogHeader("UNHANDLED EXCEPTION");
            _loggingHelper.LogCodeError("Coder application aborted", e.Message, e.StackTrace);
            _loggingHelper.CloseLog();
        }
    }


    private void CodeData(Source source, Options opts)
    {
        // Obtain source details, augment with connection string for this database.
        // Set up sf monitor tables as foreign tables, temporarily.

        Credentials creds = _monDataLayer.Credentials;
        source.db_conn = creds.GetConnectionString(source.database_name!);
        _loggingHelper.LogStudyHeader(opts, "For source: " + source.id + ": " + source.database_name!);
        _loggingHelper.LogHeader("Setup");
       
        CodingBuilder cb = new(source, opts, _loggingHelper);
        cb.EstablishContextForeignTables(creds);
        _loggingHelper.LogLine("Foreign (mon) tables established in database");
        cb.EstablishTempTables();
        
        // If pubmed (or includes pubmed, as with expected test data), do these updates first.
        
        if (source.id is 100135 or 999999)
        {
            cb.ObtainPublisherInformation();
            cb.ApplyPublisherData();
            _loggingHelper.LogLine("Updating Publisher Info\n");
        }

        // Update and standardise organisation ids and names

        if (source.has_study_tables is true || source.source_type == "test")
        {
            cb.UpdateStudyIdentifiers();
            _loggingHelper.LogLine("Study identifier orgs updated");

            if (source.has_study_organisations is true)
            {
                cb.UpdateStudyOrgs();
                _loggingHelper.LogLine("Study contributor orgs updated");
            }
            if (source.has_study_people is true)
            {
                cb.UpdateStudyPeople();
                _loggingHelper.LogLine("Study contributor orgs updated");
            }
            
            cb.StoreUnMatchedOrgNamesForStudies();
            _loggingHelper.LogLine("Unmatched org names for studies stored");
            
            if (source.has_study_countries is true)
            {
                cb.UpdateStudyCountries();
                _loggingHelper.LogLine("Study country names and codes updated");
                cb.StoreUnMatchedCountriesForStudies();
                _loggingHelper.LogLine("Unmatched country names for studies stored");
            }
            
            if (source.has_study_locations is true)
            {
                cb.UpdateStudyLocations();
                _loggingHelper.LogLine("Study location names and codes updated");
                cb.StoreUnMatchedLocationDataForStudies();
                _loggingHelper.LogLine("Unmatched location data for studies stored");
            }
            
            if (source.has_study_iec is true)
            {
                cb.UpdateStudyIEC();
                _loggingHelper.LogLine("Study inclusion and exclusion criteria updated");
            }
        }

        if (source.source_type is "object" or "test")
        {
            // works at present in the context of PubMed - may need changing 

            cb.UpdateObjectIdentifiers();
            _loggingHelper.LogLine("Object identifier orgs updated");

            cb.UpdateObjectPeople();
            _loggingHelper.LogLine("Object contributor orgs updated");
            
            cb.UpdateObjectOrganisations();
            _loggingHelper.LogLine("Object contributor orgs updated");

            cb.StoreUnMatchedNamesForObjects();
            _loggingHelper.LogLine("Unmatched org names for objects stored");
        }

        cb.UpdateDataObjectOrgs();
        _loggingHelper.LogLine("Data object managing orgs updated");
        
        cb.UpdateObjectInstanceOrgs();
        _loggingHelper.LogLine("Data object instance managing orgs updated");

        cb.StoreUnMatchedNamesForDataObjects();
        _loggingHelper.LogLine("Unmatched org names in data objects stored");

        // Update and standardise topic ids and names
        
        cb.UpdateConditions();
        _loggingHelper.LogLine("Conditions data updated");
        
        cb.UpdateTopics(source.source_type!);
        _loggingHelper.LogLine("Topic data updated");
        
        // Tidy up - 
        cb.DropTempTables();
        cb.DropContextForeignTables();
        _loggingHelper.LogLine("Foreign (mon) tables removed from database");    
    } 
}

