namespace MDR_Coder;

public class TestImporter
{
    private readonly ITestingDataLayer _testRepo;
    private readonly IMonDataLayer _monDataLayer;
    private readonly ILoggingHelper _loggingHelper;

    public TestImporter(IMonDataLayer monDataLayer, 
                        ITestingDataLayer testRepo, ILoggingHelper loggingHelper)
    {
        _monDataLayer = monDataLayer;
        _testRepo = testRepo;
        _loggingHelper = loggingHelper;
    }
    
    
    public void Run(Options opts)
    {    
        // one or both of -F, -G have been used
        // 'F' = is a test, If present, operates on the sd / ad tables in the test database
        // 'G' = test report, If present, compares and reports on adcomp versus expected tables
        // but does not recreate those tables first. -F and -G frequently used in combination
        
        _loggingHelper.OpenLogFile("test");

        if (opts.UsingTestData is true)
        {
            // First recreate the AD composite tables.
            // These will hold the AD data for all imported studies,
            // collected per source after each import, and 
            // thus available for comparison with the expected data

            //_testRepo.SetUpADCompositeTables();

            // Go through and import each test source.
            // In test context, the small 'sd' test databases are
            // imported one at a time, into equally small ad tables.

            foreach (int sourceId in opts.SourceIds!)
            {
                Source? source = _monDataLayer.FetchSourceParameters(sourceId);
                if (source is not null)
                {
                    opts.RebuildAdTables = true;
                    ImportDataInTest(source, opts);
                    _loggingHelper.LogHeader("ENDING " + sourceId.ToString() + ": " + source.database_name +
                                              " first test pass");
                }
            }

            // make scripted changes to the ad tables to
            // create diffs between them.

            //_testRepo.ApplyScriptedADChanges();

            // Go through each test source again,
            // this time keeping the ad tables.

            foreach (int sourceId in opts.SourceIds)
            {
                Source? source = _monDataLayer.FetchSourceParameters(sourceId);
                if (source is not null)
                {
                    opts.RebuildAdTables = false;
                    ImportDataInTest(source, opts);
                    _loggingHelper.LogHeader("ENDING " + sourceId.ToString() + ": " + source.database_name +
                                              " second test pass");
                }
            }
        }
        
        if (opts.CreateTestReport is true)
        {
            // construct a log detailing differences between the
            // expected and actual (composite ad) values.

            //_testRepo.ConstructDiffReport();
        }
    }
    
    
    private void ImportDataInTest(Source source, Options opts)
    {
        // Obtain source details, augment with connection string for this database.

        Credentials creds = _monDataLayer.Credentials;
        //source.db_conn = creds.GetConnectionString(source.database_name!, true);
        _loggingHelper.LogStudyHeader(opts, "For source: " + source?.id! + ": " + source?.database_name!);
        _loggingHelper.LogHeader("Setup");

        // First need to copy sd data back from composite
        // sd tables to the sd tables for this source.
        
        //_testRepo.RetrieveSDData(source!);

        // Establish top level builder classes and 
        // set up sf monitor tables as foreign tables, temporarily.

        //ImportBuilder ib = new ImportBuilder(source!, _loggingHelper);
        //DataTransferrer transferrer = new DataTransferrer(source!, _loggingHelper);
        //transferrer.EstablishForeignMonTables(creds);
        _loggingHelper.LogLine("Foreign (mon) tables established in database");

        // Recreate ad tables if necessary. If the second pass of a 
        // test loop will need to retrieve the ad data back from compad

        if (opts.RebuildAdTables is true)
        {
            //AdBuilder adb = new AdBuilder(source!, _loggingHelper);
            //adb.BuildNewAdTables();
        }
        else
        {
            //_testRepo.RetrieveADData(source!);
        }

        // Create and fill temporary tables to hold ids and edit statuses  
        // of new, edited, deleted studies and data objects.
        // Do not count deleted records on first pass, do on second.
        
        _loggingHelper.LogHeader("Start Import Process");
        _loggingHelper.LogHeader("Create and fill diff tables");
        //ib.CreateImportTables();
        bool countDeleted = opts.RebuildAdTables is not true;
        //ib.FillImportTables(countDeleted);     
        _loggingHelper.LogDiffs(source!);

        // Create import event log record and start 
        // the data transfer proper...

        int importId = _monDataLayer.GetNextImportEventId();
       // ImportEvent import = ib.CreateImportEvent(importId);

        // Consider new studies, record dates, edited studies and / or objects,
        // and any deleted studies / objects

        _loggingHelper.LogHeader("Adding new data");
        if (source!.has_study_tables is true)
        {
            //    transferrer.AddNewStudies(importId);
        }
        //transferrer.AddNewDataObjects(importId);

        _loggingHelper.LogHeader("Editing existing data where necessary");
        if (source!.has_study_tables is true)
        {
         //   transferrer.UpdateEditedStudyData(importId);
        }
        //transferrer.UpdateEditedDataObjectData(importId);

        _loggingHelper.LogHeader("Updating dates of data");
        //transferrer.UpdateDatesOfData();   
        
        _loggingHelper.LogHeader("Deleting data no longer present in source");
        if (source!.has_study_tables is true)
        {
            //    transferrer.RemoveDeletedStudyData(importId);
        }
        //transferrer.RemoveDeletedDataObjectData(importId);
        
        // Copy ad data from ad tables to the compad tables...
        // Tidy up by removing monitoring tables
        
        //_testRepo.TransferADDataToComp(source);        
        //transferrer.DropForeignMonTables();
        _loggingHelper.LogLine("Foreign (mon) tables removed from database");
    } 
}
 