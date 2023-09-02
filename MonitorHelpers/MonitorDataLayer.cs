using Dapper;
using Dapper.Contrib.Extensions;
using Npgsql;

namespace MDR_Coder;

public class MonDataLayer : IMonDataLayer
{
    private readonly ICredentials? _credentials;
    private readonly string? _connString;
    private readonly ILoggingHelper _loggingHelper;
    
    public MonDataLayer(ICredentials credentials, ILoggingHelper loggingHelper)
    {
        NpgsqlConnectionStringBuilder builder = new();
        builder.Host = credentials.Host;
        builder.Username = credentials.Username;
        builder.Password = credentials.Password;

        builder.Database = "mon";
        _connString = builder.ConnectionString;

        _credentials = credentials;
        _loggingHelper = loggingHelper;
    }

    public Credentials Credentials => (Credentials)_credentials!;
           
    public bool SourceIdPresent(int? sourceId)
    {
        string sqlString = "Select id from sf.source_parameters where id = " + sourceId.ToString();
        using NpgsqlConnection conn = new(_connString);
        int res = conn.QueryFirstOrDefault<int>(sqlString);
        return (res != 0);
    }
    
    public Source? FetchSourceParameters(int? sourceId)
    {
        using NpgsqlConnection conn = new(_connString);
        return conn.Get<Source>(sourceId);
    }

    public int GetNextCodingEventId()
    {
        using NpgsqlConnection conn = new(_connString);
        string sqlString = "select max(id) from sf.coding_events ";
        int lastId = conn.ExecuteScalar<int>(sqlString);
        return (lastId == 0) ? 10001 : lastId + 1;
    }
  
    
    public int StoreCodingEvent(CodeEvent coding)
    {
        coding.time_ended = DateTime.Now;
        using NpgsqlConnection conn = new(_connString);
        return (int)conn.Insert(coding);
    }
    
    
    public void UpdateStudiesCodedDate(int codingId, string db_conn_string)
    {
        string top_string = @"Update mn.source_data src
                  set last_coding_id = " + codingId + @", 
                  last_coded = current_timestamp
                  from 
                     (select so.sd_sid 
                     FROM ad.studies so ";
        string base_string = @" ) s
                      where s.sd_sid = src.sd_sid;";

        UpdateLastCodedDate("studies", top_string, base_string, db_conn_string);
    }


    public void UpdateObjectsCodedDate(int codingId, string db_conn_string)
    {
        string top_string = @"UPDATE mn.source_data src
                  set last_coding_id = " + codingId + @", 
                  last_coded = current_timestamp
                  from 
                     (select so.sd_oid 
                      FROM ad.data_objects so ";
        string base_string = @" ) s
                      where s.sd_oid = src.sd_oid;";

        UpdateLastCodedDate("data_objects", top_string, base_string, db_conn_string);
    }
    

    private void UpdateLastCodedDate(string tableName, string topSql, 
                                       string baseSql, string db_conn_string)
    {
        try
        {   
            using NpgsqlConnection conn = new(db_conn_string);
            string feedbackA = "Updating monitor records with date time of coding, ";
            string sqlString = $"select count(*) from ad.{tableName}";
            int recCount  = conn.ExecuteScalar<int>(sqlString);
            int recBatch = 50000;
            if (recCount > recBatch)
            {
                for (int r = 1; r <= recCount; r += recBatch)
                {
                    sqlString = topSql + 
                                " where so.id >= " + r + " and so.id < " + (r + recBatch)
                                + baseSql;
                    conn.Execute(sqlString);
                    string feedback = feedbackA + r + " to ";
                    feedback += (r + recBatch < recCount) ? (r + recBatch - 1).ToString() : recCount.ToString();
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                sqlString = topSql + baseSql;
                conn.Execute(sqlString);
                _loggingHelper.LogLine(feedbackA + recCount + " as a single query");
            }
        }
        catch (Exception e)
        {
            string res = e.Message;
            _loggingHelper.LogError("In update last coded date (" + tableName + "): " + res);
        }
    }

}