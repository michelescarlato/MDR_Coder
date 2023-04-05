using Dapper;
using Dapper.Contrib.Extensions;
using Npgsql;

namespace MDR_Coder;

public class MonDataLayer : IMonDataLayer
{
    private readonly ICredentials? _credentials;
    private readonly string? _sqlFileSelectString;
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

        _sqlFileSelectString = "select id, source_id, sd_id, remote_url, last_revised, ";
        _sqlFileSelectString += " assume_complete, download_status, local_path, last_saf_id, last_downloaded, ";
        _sqlFileSelectString += " last_harvest_id, last_harvested, last_import_id, last_imported ";
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

    
    public IEnumerable<StudyFileRecord> FetchStudyFileRecords(int? sourceId, int harvestTypeId = 1, DateTime? cutoffDate = null)
    {
        string sqlString = _sqlFileSelectString!;
        sqlString += " from sf.source_data_studies ";
        sqlString += GetWhereClause(sourceId, harvestTypeId, cutoffDate);

        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        return conn.Query<StudyFileRecord>(sqlString);
    }


    public IEnumerable<ObjectFileRecord> FetchObjectFileRecords(int? sourceId, int harvestTypeId = 1, DateTime? cutoffDate = null)
    {
        string sqlString = _sqlFileSelectString!;
        sqlString += " from sf.source_data_objects";
        sqlString += GetWhereClause(sourceId, harvestTypeId, cutoffDate);

        using NpgsqlConnection conn = new(_connString);
        return conn.Query<ObjectFileRecord>(sqlString);
    }


    public int FetchFileRecordsCount(int? sourceId, string sourceType, int harvestTypeId = 1, DateTime? cutoffDate = null)
    {
        string sqlString = "select count(*) ";
        sqlString += sourceType.ToLower() == "study" ? "from sf.source_data_studies"
                                             : "from sf.source_data_objects";
        sqlString += GetWhereClause(sourceId, harvestTypeId, cutoffDate);

        using NpgsqlConnection conn = new(_connString);
        return conn.ExecuteScalar<int>(sqlString);
    }


    public IEnumerable<StudyFileRecord> FetchStudyFileRecordsByOffset(int? sourceId, int offsetNum,
                                  int amount, int harvestTypeId = 1, DateTime? cutoffDate = null)
    {
        string sqlString = _sqlFileSelectString!;
        sqlString += " from sf.source_data_studies ";
        sqlString += GetWhereClause(sourceId, harvestTypeId, cutoffDate);  
        sqlString += " offset " + offsetNum + " limit " + amount;

        using NpgsqlConnection conn = new NpgsqlConnection(_connString);
        return conn.Query<StudyFileRecord>(sqlString);
    }

    public IEnumerable<ObjectFileRecord> FetchObjectFileRecordsByOffset(int? sourceId, int offsetNum,
                                 int amount, int harvestTypeId = 1, DateTime? cutoffDate = null)
    {
        string sqlString = _sqlFileSelectString!;
        sqlString += " from sf.source_data_objects ";
        sqlString += GetWhereClause(sourceId, harvestTypeId, cutoffDate);
        sqlString += " offset " + offsetNum + " limit " + amount;

        using NpgsqlConnection conn = new(_connString);
        return conn.Query<ObjectFileRecord>(sqlString);
    }

    private string GetWhereClause(int? sourceId, int harvestTypeId, DateTime? cutoffDate = null)
    {
        string whereClause = "";
        if (harvestTypeId == 1)
        {
            // Count all files.
            whereClause = " where source_id = " + sourceId.ToString();
        }
        else if (harvestTypeId == 2)
        {
            // Count only those files that have been revised (or added) on or since the cutoff date.
            whereClause = " where source_id = " + sourceId.ToString() + " and last_revised >= '" + cutoffDate + "'";
        }
        else if (harvestTypeId == 3)
        {
            // For sources with no revision date - Count files unless assumed complete has been set
            // as true (default is null) in which case no further change is expected.
            whereClause = " where source_id = " + sourceId.ToString() + " and assume_complete is null";
        }

        whereClause += " and local_path is not null";
        whereClause += " order by local_path";
        return whereClause;
    }

    // get record of interest
    public StudyFileRecord? FetchStudyFileRecord(string sdId, int? sourceId, string sourceType)
    {
        using NpgsqlConnection conn = new(_connString);
        string sqlString = _sqlFileSelectString!;
        sqlString += " from sf.source_data_studies";
        sqlString += " where sd_id = '" + sdId + "' and source_id = " + sourceId.ToString();
        return conn.Query<StudyFileRecord>(sqlString).FirstOrDefault();
    }


    public ObjectFileRecord? FetchObjectFileRecord(string sdId, int? sourceId, string sourceType)
    {
        using NpgsqlConnection conn = new(_connString);
        string sqlString = _sqlFileSelectString!;
        sqlString += " from sf.source_data_objects";
        sqlString += " where sd_id = '" + sdId + "' and source_id = " + sourceId.ToString();
        return conn.Query<ObjectFileRecord>(sqlString).FirstOrDefault();
    }

    public int StoreCodingEvent(CodeEvent coding)
    {
        coding.time_ended = DateTime.Now;
        using NpgsqlConnection conn = new(_connString);
        return (int)conn.Insert(coding);
    }
    
    public void UpdateStudiesLastCodedDate(int codingId, int? sourceId)
    {
        string top_string = @"Update mn.source_data src
                      set last_coding_id = " + codingId.ToString() + @", 
                      last_coded = current_timestamp
                      from 
                         (select so.id, so.sd_sid 
                         FROM sd.studies so
                         INNER JOIN sd.to_ad_study_recs ts
                         ON so.sd_sid = ts.sd_sid
                         where ts.status in (1, 2, 3) 
                         ";
        string base_string = @" ) s
                          where s.sd_sid = src.sd_id and
                          src.source_id = " + sourceId.ToString();

        UpdateLastCodedDate("studies", top_string, base_string);
    }

    
    public void UpdateObjectsLastCodedDate(int importId, int? sourceId)
    {
        string top_string = @"UPDATE mon.source_data src
                      set last_import_id = " + importId.ToString() + @", 
                      last_imported = current_timestamp
                      from 
                         (select so.id, so.sd_oid 
                          FROM sd.data_objects so
                          INNER JOIN sd.to_ad_object_recs ts
                          ON so.sd_oid = ts.sd_oid
                          where ts.status in (1, 2, 3) 
                         ";
        string base_string = @" ) s
                          where s.sd_oid = src.sd_id and
                          src.source_id = " + sourceId.ToString();

        UpdateLastCodedDate("data_objects", top_string, base_string);
    }


    private void UpdateLastCodedDate(string tableName, string topSql, string baseSql)
    {
        try
        {   
            using NpgsqlConnection conn = new(_connString);
            string feedbackA = $"Updating monitor import records, (mon.source_data_{tableName}), ";
            string sqlString = $"select count(*) from sd.{tableName}";
            int recCount  = conn.ExecuteScalar<int>(sqlString);
            int recBatch = 100000;
            if (recCount > recBatch)
            {
                for (int r = 1; r <= recCount; r += recBatch)
                {
                    sqlString = topSql + 
                                 " and so.id >= " + r + " and so.id < " + (r + recBatch).ToString() 
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
                _loggingHelper.LogLine(feedbackA + recCount + " records, as a single batch");
            }
        }
        catch (Exception e)
        {
            string res = e.Message;
            _loggingHelper.LogError("In update last imported date (" + tableName + "): " + res);
        }
    }

}