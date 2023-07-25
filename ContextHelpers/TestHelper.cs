using Dapper;
using Npgsql;
namespace MDR_Coder;

public class TestHelper
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly string _db_conn;
    
    public TestHelper(Source source, ILoggingHelper loggingHelper)
    {
        _loggingHelper = loggingHelper;
        _db_conn = source.db_conn!;
    }
    
    public void EstablishTempStudyTestList()
    {
        // List has to be derived from the mn.source dta 'for testing' studies
        // but they also have to be present in the sd data
        
        using var conn = new NpgsqlConnection(_db_conn);
        string sql_string = @"DROP TABLE IF EXISTS mn.test_study_list;
                        CREATE TABLE mn.test_study_list as 
                        SELECT sd.sd_sid from mn.source_data sd
                        inner join ad.studies s
                        on sd.sd_sid = s.sd_sid
                        WHERE sd.for_testing = true";
        conn.Execute(sql_string);

        sql_string = @"DROP TABLE IF EXISTS mn.test_object_list;
                            CREATE TABLE mn.test_object_list as 
                            SELECT sd_oid from ad.data_objects sdo
                            inner join mn.test_study_list tsl
                            on sdo.sd_sid = tsl.sd_sid";
        conn.Execute(sql_string);
        _loggingHelper.LogLine("Temp test study list created");
    }
   
    public void EstablishTempObjectTestList()
    {
        // List has to be derived from the mn.source data 'for testing' objects
        // but they also have to be present in the sd data
        
        using var conn = new NpgsqlConnection(_db_conn);
        string sql_string = @"DROP TABLE IF EXISTS mn.test_object_list;
                        CREATE TABLE mn.test_object_list as 
                        SELECT sd.sd_oid from mn.source_data sd
                        inner join ad.data_objects s
                        on sd.sd_oid = s.sd_oid
                        WHERE sd.for_testing = true";
        conn.Execute(sql_string);
        
        sql_string = @"select count(*) from mn.test_object_list";
        _loggingHelper.LogLine("Temp test object list created");
    }
    
    public void TeardownTempTestDataTables()
    {
        string sql_string = @"DROP TABLE IF EXISTS mn.test_study_list;
                            DROP TABLE IF EXISTS mn.test_object_list;";
        using var conn = new NpgsqlConnection(_db_conn);
        conn.Execute(sql_string);
        _loggingHelper.LogLine("Temp test lists dropped");
    }
}