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
        
        // If pubmed and publisher updates requested, do these updates first.
        
        if (opts.RecodeAllPublishers || opts.RecodeUnmatchedPublishers)
        {
            if (source.id is 100135)
            {
                cb.ObtainPublisherInformation();
                cb.ApplyPublisherData();
            }
        }

        // Update and standardise study and object organisation ids and names
        
        if (opts.RecodeAllOrgs || opts.RecodeUnmatchedOrgs)
        {
            if (source.has_study_tables is true)
            {
                cb.UpdateStudyIdentifiers();
                cb.UpdateStudyOrgs();   // table existence checked later
                cb.UpdateStudyPeople();   // table existence checked later
                
                cb.UpdateDataObjectOrgs();
                cb.UpdateObjectInstanceOrgs();      
               
                if (source.has_object_pubmed_set is true)
                {
                    cb.UpdateObjectIdentifiers();
                    cb.UpdateObjectPeople();
                    cb.UpdateObjectOrganisations();
                }
            }
        }
        
        
        // Update and standardise study countries and locations.
        
        if (opts.RecodeAllLocations || opts.RecodeUnmatchedLocations)
        {
                cb.UpdateStudyCountries();
                cb.UpdateStudyLocations();   
        }
        
        // Update and standardise topic ids and names

        if (opts.RecodeAllTopics || opts.RecodeUnmatchedTopics)
        {
            cb.UpdateTopics(source.source_type!);
        }
        
        // Update and standardise condition ids and names
        
        if (opts.RecodeAllConditions || opts.RecodeUnmatchedConditions)
        {
            cb.UpdateConditions();
        }
        
        
        // Tidy up 
        
        if (source.has_study_tables is true)
        {
            _monDataLayer.UpdateStudiesCodedDate(codingId, source.db_conn);
        }
        else
        {
            // only do the objects table if there are no studies (e.g. PubMed).
            
            _monDataLayer.UpdateObjectsCodedDate(codingId, source.db_conn);
        }
        _monDataLayer.StoreCodingEvent(coding);
        
        cb.DropTempTables();
        cb.DropContextForeignTables();
        _loggingHelper.LogLine("Foreign (mon) tables removed from database");    
    } 
}

