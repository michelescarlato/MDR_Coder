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
        
        // if test data only create the test data study and object lists
        
        TestHelper th = new TestHelper(source, _loggingHelper);
        if (opts.ReCodeTestDataOnly)
        {
            if (source.source_type == "study")
            {
                th.EstablishTempStudyTestList();
            }
            else
            {
                th.EstablishTempObjectTestList();
            }
        }

        // If pubmed and publisher updates requested, do these updates first.
        
        if (opts.RecodePublishers > 0 && source.has_object_pubmed_set is true)
        {
            cb.ObtainPublisherInformation();
            cb.ApplyPublisherData();
        }

        // Update and standardise study and object organisation ids and names
        
        if (opts.RecodeOrgs > 0)
        {
            if (source.has_study_tables is true)
            {
                cb.UpdateStudyIdentifiers();
                if (source.has_study_people is true)
                {
                    cb.UpdateStudyPeople(); 
                }
                if (source.has_study_organisations is true)
                {
                    cb.UpdateStudyOrgs();
                }

                cb.UpdateDataObjectOrgs();
                cb.UpdateObjectInstanceOrgs();      
            }
            
            if (source.has_object_pubmed_set is true)
            {
                cb.UpdateDataObjectOrgs();
                cb.UpdateObjectInstanceOrgs(); 
                
                cb.UpdateObjectIdentifiers();
                cb.UpdateObjectPeople();
                cb.UpdateObjectOrganisations();
            }
        }
        
        
        // Update and standardise study countries and locations.
        
        if (opts.RecodeLocations > 0)
        {
                cb.UpdateStudyCountries();
                cb.UpdateStudyLocations();   
        }
        
        // Update and standardise topic ids and names

        if (opts.RecodeTopics > 0)
        {
            cb.UpdateTopics(source.source_type!);
        }
        
        // Update and standardise condition ids and names
        
        if (opts.RecodeConditions > 0)
        {
            cb.UpdateConditions();
        }
        
        // Tidy up 
        
        cb.DropTempOrgTables();
        if (opts.ReCodeTestDataOnly)
        {
            th.TeardownTempTestDataTables();
        }
        else
        {
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
        }

        cb.DropContextForeignTables();
        _loggingHelper.LogLine("Foreign (mon) tables removed from database");    
    } 
}

