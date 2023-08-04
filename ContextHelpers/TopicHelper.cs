using Dapper;
using Npgsql;

namespace MDR_Coder;

public class TopicHelper
{
    private readonly string _db_conn;
    private readonly ILoggingHelper _loggingHelper;
    private readonly bool _recodeTestDataOnly;
    private readonly string topic_table;
    private readonly string topic_type;
    private string scope_qualifier;
    private string fb_qualifier;
    private bool? has_conditions_data;

    public TopicHelper(Source source, ILoggingHelper logger, int scope, bool recodeTestDataOnly)
    {
        _db_conn = source.db_conn ?? "";
        _loggingHelper = logger;
        _recodeTestDataOnly = recodeTestDataOnly;        
        topic_table = source.source_type!.ToLower() == "study" ? "study_topics" : "object_topics";
        topic_type = source.source_type!.ToLower();
        scope_qualifier = scope == 1 ? " and t.coded_on is null " : "";
        fb_qualifier = scope == 1 ? "unmatched" : "all";
        has_conditions_data = source.has_study_conditions;   // N.B. No condition data for Pubmed at present
    }

    public int ExecuteSQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.Execute(sql_string);
    }
    
    private int GetMinId(string schema, string table_name)
    {
        string sql_string = @$"select min(id) from {schema}.{table_name};";
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
    }

    private int GetMaxId(string schema, string table_name)
    {
        string sql_string = @$"select max(id) from {schema}.{table_name};";
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
    }
    
    private int GetTableCount(string schema, string table_name)
    {
        // gets the total numbers of records in the table (even if not all will be updated,    
        // they will all be included in the query).                                            
        
        string sql_string = $"select count(*) from {schema}.{table_name};";
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
    }
    
    private int GetFieldCount(string schema, string table_name, string field_name)
    {
        // gets the total numbers of records in the table (even if not all will be updated,    
        // they will all be included in the query).                                            
        
        string sql_string = @$"select count(*) from {schema}.{table_name}
                               where {field_name} is not null;";
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
    }
    
    private void FeedbackTopicResults(string schema, string table_name, string id_field)
    {
        int table_count = GetTableCount(schema, table_name);
        int coded_count = GetFieldCount(schema, table_name, id_field);
        _loggingHelper.LogLine($"{coded_count} records, from {table_count}, " +
                               $"{(double) 100 * coded_count / table_count:N1} %, " +  
                               $"have MESH coded topics in {schema}.{table_name}");
    }

    public void process_topics()
    {
        int min_id = GetMinId("ad", topic_table);
        int max_id = GetMaxId("ad", topic_table);
        delete_no_information_categories(min_id, max_id, 100000);
        if (has_conditions_data == true)
        {
            identify_conditions(min_id, max_id, 100000);
        }
        identify_geographic(min_id, max_id, 100000);
        match_to_mesh_topics(min_id, max_id, 100000);
        FeedbackTopicResults("ad",topic_table, "mesh_code");
    }

    
    private void delete_no_information_categories(int min_id, int max_id, int rec_batch)
    {
        string top_string = $"delete from ad.{topic_table} t";
        
        if (_recodeTestDataOnly)    // test data 'trumps' the decisions above 
        {
            scope_qualifier = topic_type == "study" 
                        ? " and t.sd_sid in (select sd_sid from mn.test_study_list) "
                        : " and t.sd_oid in (select sd_oid from mn.test_object_list) ";
            fb_qualifier = "test data";
        }
        
        string sql_string = top_string + $@" where (lower(original_value) = '' 
                        or lower(original_value) = 'human'
                        or lower(original_value) = 'humans'
                        or lower(original_value) = 'other'
                        or lower(original_value) = 'studies'
                        or lower(original_value) = 'evaluation'    
                        or lower(original_value) = 'n/a') 
                        {scope_qualifier}";
        delete_topics(sql_string, "A", min_id, max_id, rec_batch);

        sql_string = top_string + $@" where (lower(original_value) = 'healthy adults' 
                        or lower(original_value) = 'healthy adult'
                        or lower(original_value) = 'healthy person'
                        or lower(original_value) = 'healthy people'
                        or lower(original_value) = 'female'
                        or lower(original_value) = 'male'
                        or lower(original_value) = 'healthy adult female'
                        or lower(original_value) = 'healthy adult male') 
                        {scope_qualifier}";
        delete_topics(sql_string, "B", min_id, max_id, rec_batch);

        sql_string = top_string + $@" where (lower(original_value) = 'hv' 
                        or lower(original_value) = 'healthy volunteer'
                        or lower(original_value) = 'healthy volunteers'
                        or lower(original_value) = 'volunteer'
                        or lower(original_value) = 'healthy control'
                        or lower(original_value) = 'normal control') 
                        {scope_qualifier}";
        delete_topics(sql_string, "C", min_id, max_id, rec_batch);

        sql_string = top_string + $@" where (lower(original_value) = 'healthy individual' 
                        or lower(original_value) = 'healthy individuals'
                        or lower(original_value) = 'n/a(healthy adults)'
                        or lower(original_value) = 'n/a (healthy adults)'
                        or lower(original_value) = 'none (healthy adults)'
                        or lower(original_value) = 'healthy older adults'
                        or lower(original_value) = 'healthy japanese subjects') 
                        {scope_qualifier}";
        delete_topics(sql_string, "D", min_id, max_id, rec_batch);

        sql_string = top_string + $@" where (lower(original_value) = 'intervention' 
                        or lower(original_value) = 'implementation'
                        or lower(original_value) = 'prediction'
                        or lower(original_value) = 'recovery'
                        or lower(original_value) = 'healthy'
                        or lower(original_value) = 'complications') 
                        {scope_qualifier}";
        delete_topics(sql_string, "E", min_id, max_id, rec_batch);

        sql_string = top_string + $@" where (lower(original_value) = 'process evaluation' 
                        or lower(original_value) = 'follow-up'
                        or lower(original_value) = 'validation'
                        or lower(original_value) = 'tolerability'
                        or lower(original_value) = 'training'
                        or lower(original_value) = 'refractory') 
                        {scope_qualifier}";
        delete_topics(sql_string, "F", min_id, max_id, rec_batch);

        sql_string = top_string + $@" where (lower(original_value) = 'symptoms' 
                        or lower(original_value) = 'clinical research/ practice'
                        or lower(original_value) = 'predictors'
                        or lower(original_value) = 'management'
                        or lower(original_value) = 'disease'
                        or lower(original_value) = 'relapsed') 
                        {scope_qualifier}";
        delete_topics(sql_string, "G", min_id, max_id, rec_batch);

        sql_string = top_string + $@" where (lower(original_value) = 'complication' 
                        or lower(original_value) = '-'
                        or lower(original_value) = 'prep'
                        or lower(original_value) = 'not applicable'
                        or lower(original_value) = 'function'
                        or lower(original_value) = 'toxicity' 
                        or lower(original_value) = 'health condition 1: o- medical and surgical') 
                        {scope_qualifier}";
        delete_topics(sql_string, "H", min_id, max_id, rec_batch);
    }


    private void delete_topics(string sql_string, string delete_set, 
                              int min_id, int max_id, int rec_batch)
    {
        // Normally only applies to newly added topic records or records that have not been coded in the past
        // (coded_on = null). Previous topic records will already have been filtered by this process.
        string feedback_core = $"'no information' {topic_type} topics (group {delete_set}) -";
        try
        {
            if (max_id - min_id > rec_batch)
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " and t.id >= " + r + " and t.id < " + (r + rec_batch);
                    int res_r = ExecuteSQL(batch_sql_string);
                    if (res_r > 0)
                    {
                        int e =  r + rec_batch < max_id ? r + rec_batch : max_id;
                        _loggingHelper.LogLine($"Deleting {res_r} {feedback_core} in {fb_qualifier} topics, ids {r} to {e}");
                    }
                }
            }
            else
            {
                int res =  ExecuteSQL(sql_string);
                if (res > 0)
                {
                    _loggingHelper.LogLine($"Deleting {res} {feedback_core} in {fb_qualifier} topics, as a single query");
                }
            }
        }
        catch (Exception e)
        {
            string eres = e.Message;
            _loggingHelper.LogError("In deleting 'no information' categories: " + eres);
        }
    }

    private void identify_conditions(int min_id, int max_id, int rec_batch)
    {
        // Normally only applies to newly added topic records or records that have not been coded in the past.
        // Previous topic records should have already have been filtered by this process.
        
        if (_recodeTestDataOnly)    // test data 'trumps' the decisions above 
        {
            scope_qualifier = topic_type == "study" 
                ? " and t.sd_sid in (select sd_sid from mn.test_study_list) "
                : " and t.sd_oid in (select sd_oid from mn.test_object_list) ";
            fb_qualifier = "test data";
        }
        
        // Initially identify the icd-codable topics
        string sql_string = @"DROP TABLE IF EXISTS ad.temp_condition_topic_ids;
                            CREATE TABLE ad.temp_condition_topic_ids 
                            (
                               id int, sd_sid varchar, original_value varchar, ct_id int, ct_code varchar,
                               icd_code varchar, icd_term varchar
                            );";
        ExecuteSQL(sql_string);
        
        try
        {
            if (max_id - min_id > rec_batch)
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    // create a list of relevant topic records, add them to the conditions table, 
                    // remove them from the topics table   
                    
                    identify_conditions(r, rec_batch, max_id, scope_qualifier, fb_qualifier);
                    add_new_conditions(fb_qualifier);
                    delete_condition_topics(r, rec_batch, max_id, fb_qualifier);;
                }
            }
            else
            {
                // create a list of relevant topic records, add them to the conditions table, 
                // remove them from the topics table   
                    
                identify_conditions(0, 0, 0, scope_qualifier, fb_qualifier);
                add_new_conditions(fb_qualifier);
                delete_condition_topics(0, 0, 0, fb_qualifier);
            }

            sql_string = @"DROP TABLE IF EXISTS ad.temp_condition_topic_ids;";
            ExecuteSQL(sql_string);
        }
        
        catch (Exception e)
        {
            string eres = e.Message;
            _loggingHelper.LogError("In transferring condition topics: " + eres);
        }
    }

    private void identify_conditions(int r, int rec_batch, int max_id, string scope, string fb_qual)
    {
        string batch_sql_string = rec_batch > 0 ? " and t.id >= " + r + " and t.id < " + (r + rec_batch) : "";
        const string feedback_core = "condition term";
        
        string sql_string = $@"TRUNCATE TABLE ad.temp_condition_topic_ids;
                            INSERT INTO ad.temp_condition_topic_ids(id, sd_sid, original_value, ct_id, ct_code, icd_code, icd_term)
                            SELECT t.id, t.sd_sid, t.original_value, t.original_ct_type_id, t.original_ct_code, m.icd_code, m.icd_term
                            from ad.study_topics t inner join
                            context_ctx.icd_terms_lookup m
                            on lower(t.original_value) = lower(m.entry_term)
                            {batch_sql_string} {scope}";
        int res = ExecuteSQL(sql_string);
        if (res > 0)
        {
            if (rec_batch > 0)
            {
                int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                _loggingHelper.LogLine($"Identifying {res} {feedback_core} in {fb_qual} topics, in ids {r} to {e}");
            }
            else
            {
                _loggingHelper.LogLine($"Identifying {res} {feedback_core} in {fb_qual} topics, as a single query");
            }
        }
    }
    
    private void add_new_conditions(string fb_qual)
    {
        const string feedback_core = "condition term records";
        string sql_string = $@"INSERT INTO ad.study_conditions(sd_sid, original_value, original_ct_type_id, original_ct_code, icd_code, 
                            icd_name, coded_on)
                            SELECT sd_sid, original_value, ct_id, ct_code, icd_code, icd_term, now()
                            from ad.temp_condition_topic_ids ";
        int res = ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"Adding {res} {feedback_core} from {fb_qual} topics, as a single query");
    }
    
    
    private void delete_condition_topics(int r, int rec_batch, int max_id, string fb_qual)
    {
        string batch_sql_string = rec_batch > 0 ? " and t.id >= " + r + " and t.id < " + (r + rec_batch) : "";
        const string feedback_core = "condition topic records";
        
        string sql_string = $@"DELETE from ad.study_topics t
                            USING ad.temp_condition_topic_ids tc
                            where t.id = tc.id {batch_sql_string}";
        int res = ExecuteSQL(sql_string);
        if (res > 0)
        {
            if (rec_batch > 0)
            {
                int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                _loggingHelper.LogLine($"Deleting {res} {feedback_core} from {fb_qual} topics, in ids {r} to {e}");
            }
            else
            {
                _loggingHelper.LogLine($"Deleting {res} {feedback_core} from {fb_qual} topics, as a single query");
            }
        }
    }
    
    private void identify_geographic(int min_id, int max_id, int rec_batch)
    {
        // Normally only applies to newly added topic records or records that have not been coded in the past.
        // Previous topic records will already have been filtered by this process.
        
        if (_recodeTestDataOnly)    // test data 'trumps' the decisions above 
        {
            scope_qualifier = topic_type == "study" 
                ? " and t.sd_sid in (select sd_sid from mn.test_study_list) "
                : " and t.sd_oid in (select sd_oid from mn.test_object_list) ";
            fb_qualifier = "test data";
        }
        
        string sql_string = $@"update ad.{topic_table} t 
                               set topic_type_id = 16
                               from context_ctx.country_names g
                               where t.original_value = g.alt_name 
                               {scope_qualifier}";
        
        string feedback_core = $"geographic {topic_type} topics -";
        try
        {
            if (max_id - min_id > rec_batch)
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " and t.id >= " + r + " and t.id < " + (r + rec_batch);
                    int res_r = ExecuteSQL(batch_sql_string);
                    if (res_r > 0)
                    {
                        int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                        _loggingHelper.LogLine($"Identifying {res_r} {feedback_core} in {fb_qualifier} topics, in ids {r} to {e}");
                    }
                }
            }
            else
            {
                int res = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Identifying {res} {feedback_core} in {fb_qualifier} topics, as a single query");
            }
        }
        catch (Exception e)
        {
            string eres = e.Message;
            _loggingHelper.LogError("In identifying geographic topics: " + eres);
        }
    }


    private void match_to_mesh_topics(int min_id, int max_id, int rec_batch)
    {
        if (_recodeTestDataOnly)    // test data 'trumps' the decisions above 
        {
            scope_qualifier = topic_type == "study" 
                ? " and t.sd_sid in (select sd_sid from mn.test_study_list) "
                : " and t.sd_oid in (select sd_oid from mn.test_object_list) ";
            fb_qualifier = "test data";
        }
        
        string sql_string = $@"Update ad.{topic_table} t 
                               set mesh_code = m.code,
                               mesh_value = m.term,
                               coded_on = CURRENT_TIMESTAMP
                               from context_ctx.mesh_lookup m
                               where lower(t.original_value) = m.entry 
                               {scope_qualifier}";

        string feedback_core = $"{topic_type} topic codes ";
        try
        {
            if (max_id - min_id > rec_batch)
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " and t.id >= " + r + " and t.id < " + (r + rec_batch);
                    int res_r = ExecuteSQL(batch_sql_string);
                    int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                    string feedback = $"Updating {res_r} {feedback_core} in {fb_qualifier} topics, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                int res = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Updating {res} {feedback_core} in {fb_qualifier} topics, as a single query");
            }
        }
        catch (Exception e)
        {
            string eres = e.Message;
            _loggingHelper.LogError("In updating topics: " + eres);
        }
    }
    
    public int store_unmatched_topic_values(int source_id)
    {
        string sql_string = @"delete from context_ctx.to_match_topics where source_id = " + source_id ;
        ExecuteSQL(sql_string);
        sql_string = $@"insert into context_ctx.to_match_topics (source_id, topic_value, number_of) 
                   select {source_id}, original_value, count(original_value) from ad.{topic_table} t
                   where t.mesh_code is null 
                   group by t.original_value;";
        int res = ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"Storing {res} topic codes not matched to MESH codes for review");
        return res;
    }
}
