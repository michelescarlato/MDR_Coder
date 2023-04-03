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
        int codingId = _monDataLayer.GetNextCodingEventId();
        CodeEvent coding = cb.CreateCodingEvent(codingId);  
        cb.EstablishTempTables();
        
        // If pubmed (or includes pubmed, as with expected test data), do these updates first.
        
        if (source.id is 100135 or 999999)
        {
            cb.ObtainPublisherInformation();
            cb.ApplyPublisherData();
        }

        // Update and standardise organisation ids and names

        if (source.has_study_tables is true || source.source_type == "test")
        {
            cb.UpdateStudyIdentifiers();

            if (source.has_study_organisations is true)
            {
                cb.UpdateStudyOrgs();
            }
            if (source.has_study_people is true)
            {
                cb.UpdateStudyPeople();
            }
            
            if (source.has_study_countries is true)
            {
                cb.UpdateStudyCountries();
            }
            
            if (source.has_study_locations is true)
            {
                cb.UpdateStudyLocations();
            }
            
            if (source.has_study_iec is true)
            {
                cb.UpdateStudyIEC();
            }
        }

        cb.UpdateDataObjectOrgs();
        cb.UpdateObjectInstanceOrgs();        
        
        if (source.source_type is "test" || source.has_object_pubmed_set is true)
        {
            cb.UpdateObjectIdentifiers();
            cb.UpdateObjectPeople();
            cb.UpdateObjectOrganisations();
        }


        // Update and standardise topic ids and names. and condition ids and names
        
        cb.UpdateTopics(source.source_type!);       
        cb.UpdateConditions();
        
        // Tidy up 
        
        if (source.has_study_tables is true)
        {
            cb.UpdateStudiesImportedDateInMon(codingId);
        }
        else
        {
            // only do the objects table if there are no studies (e.g. PubMed)
            cb.UpdateObjectsImportedDateInMon(codingId);
        }
        _monDataLayer.StoreCodingEvent(coding);
        
        cb.DropTempTables();
        cb.DropContextForeignTables();
        _loggingHelper.LogLine("Foreign (mon) tables removed from database");    
    } 
}

