using Dapper;
using Npgsql;

namespace MDR_Coder;

public class TestingDataLayer : ITestingDataLayer
{
    private readonly string _db_conn;
    private readonly ILoggingHelper _loggingHelper;

    public TestingDataLayer(ICredentials credentials, ILoggingHelper loggingHelper)
    {
        _loggingHelper = loggingHelper;
        _db_conn = credentials.GetConnectionString("test", 1);
    }

    public int EstablishExpectedData()
    {
        _loggingHelper.OpenLogFile("test");

        try
        {
            _loggingHelper.LogLine("STARTING EXPECTED DATA ASSEMBLY");

            //TestSchemaBuilder tsb = new TestSchemaBuilder(_db_conn);

            //tsb.SetUpMonSchema();
            _loggingHelper.LogLine("mon_sf link established");

            //tsb.SetUpExpectedTables();
            _loggingHelper.LogLine("Expected Data tables recreated");

            //tsb.SetUpSDCompositeTables();
            _loggingHelper.LogLine("SD composite test data tables recreated");

           //ExpectedDataBuilder edb = new ExpectedDataBuilder(_db_conn);

            //edb.InitialiseTestStudiesList();
            //edb.InitialiseTestPubMedObjectsList();
            _loggingHelper.LogLine("List of test studies and pubmed objects inserted");

            //edb.LoadInitialInputTables();
            _loggingHelper.LogLine("Data loaded from manual inspections");  

            //edb.CalculateAndAddOIDs();
            //_loggingHelper.Information("OIDs calculated and inserted");

            //tsb.TearDownForeignSchema();
            _loggingHelper.LogLine("mon_sf link deleted");

            return 0;
        }

        catch (Exception e)
        {
            _loggingHelper.LogCodeError("Error in establishing test data from stored procedures", e.Message, e.StackTrace);
            _loggingHelper.LogLine("Closing Log");
            _loggingHelper.CloseLog();
            return -1;
        }
    }

    public void TransferTestSDData(Source source)
    {
        //TransferSDDataBuilder tdb = new TransferSDDataBuilder(source);

        if (source.has_study_tables is true)
        {
            //tdb.DeleteExistingStudyData();
            //tdb.TransferStudyData();  
            _loggingHelper.LogLine("New study SD test data for source " + source.id + " added to CompSD");
        }

        //tdb.DeleteExistingObjectData();
        //tdb.TransferObjectData();
        _loggingHelper.LogLine("New object SD test data for source " + source.id + " added to CompSD");
    }

    public IEnumerable<int> ObtainTestSourceIDs()
    {
        string sql_string = @"select distinct source_id 
                                 from expected.source_studies
                                 union
                                 select distinct source_id 
                                 from expected.source_objects;";

        using var conn = new NpgsqlConnection(_db_conn);
        return conn.Query<int>(sql_string);
    }

}
