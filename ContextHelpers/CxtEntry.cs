namespace MDR_Coder;

public class ContextMain
{
    private readonly ILoggingHelper _loggingHelper;
    
    public ContextMain(ILoggingHelper loggingHelper)
    {
        _loggingHelper = loggingHelper;
    }

    public void UpdateDataFromContext(Credentials creds, Source source)
    {
        // Used to set up the parameters required for the procedures
        // and then run each of them

        PostProcBuilder ppb = new PostProcBuilder(source, _loggingHelper);
        ppb.EstablishContextForeignTables(creds);
        ppb.EstablishTempNamesTable();

        // if pubmed (or includes pubmed, as with expected test data), do these updates first
        if (source.id == 100135 || source.id == 999999)
        {
            ppb.ObtainPublisherInformation();
            ppb.ApplyPublisherData();
            _loggingHelper.LogLine("Updating Publisher Info\n");
        }

        // Update and standardise organisation ids and names

        if (source.has_study_tables is true || source.source_type == "test")
        {
            ppb.UpdateStudyIdentifierOrgs();
            _loggingHelper.LogLine("Study identifier orgs updated");

            if (source.has_study_contributors is true)
            {
                ppb.UpdateStudyContributorOrgs();
                _loggingHelper.LogLine("Study contributor orgs updated");
            }

            ppb.StoreUnMatchedNamesForStudies();
            _loggingHelper.LogLine("Unmatched org names for studies stored");
        }

        if (source.source_type == "object" || source.source_type == "test")
        {
            // works at present in the context of PubMed - may need changing 

            ppb.UpdateObjectIdentifierOrgs();
            _loggingHelper.LogLine("Object identifier orgs updated");

            ppb.UpdateObjectContributorOrgs();
            _loggingHelper.LogLine("Object contributor orgs updated");

            ppb.StoreUnMatchedNamesForObjects();
            _loggingHelper.LogLine("Unmatched org names for objects stored");
        }

        ppb.UpdateDataObjectOrgs();
        _loggingHelper.LogLine("Data object managing orgs updated");

        ppb.StoreUnMatchedNamesForDataObjects();
        _loggingHelper.LogLine("Unmatched org names in data objects stored");


        // Update and standardise topic ids and names
        ppb.UpdateTopics(source.source_type);
        _loggingHelper.LogLine("Topic data updated");

        // Tidy up...
        ppb.DropTempNamesTable();
        ppb.DropContextForeignTables();

    }
}