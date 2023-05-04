using Dapper;
using Npgsql;

namespace MDR_Coder;

public class TopicHelper
{
    private readonly string _db_conn;
    private readonly string _source_type;
    private readonly ILoggingHelper _loggingHelper;        

    public TopicHelper(Source source, ILoggingHelper logger)
    {
        _db_conn = source.db_conn ?? "";
        _source_type = source.source_type ?? "";
        _loggingHelper = logger;
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

    public void process_topics(bool code_all)
    {
        // The presence of a study_topics table has already been checked

        string schema = "ad";
        string topic_table = _source_type.ToLower() == "study" ? "study_topics" : "object_topics";
        
        if (!code_all) // keep a record of topic record ids not yet coded
        {
            // Keep a temp table record of topic record ids that are not yet coded.  
            // Necessary to help with de-duplication process after coding has completed.

            string sql_string = $@"drop table if exists ad.uncoded_topic_records;          
                create table ad.uncoded_topic_records as                    
                select id from ad.{topic_table} where coded_on is null; ";
            using var conn = new NpgsqlConnection(_db_conn);
            conn.Execute(sql_string);
        }
        
        int min_id = GetMinId(schema, topic_table);
        int max_id = GetMaxId(schema, topic_table);
        delete_no_information_cats(topic_table, schema, min_id, max_id, 200000, code_all);
        identify_geographic(topic_table, schema, min_id, max_id, 200000, code_all);
        mesh_match_topics(topic_table, schema, min_id, max_id, 200000, code_all);
        mesh_delete_duplicates(topic_table, schema, min_id, max_id, 50000, code_all);
        FeedbackTopicResults(schema, topic_table, "mesh_code");
    }

    
    public void delete_no_information_cats(string topic_table, string schema, 
                                           int min_id, int max_id, int rec_batch, bool code_all)
    {
        string top_string = $"delete from {schema}.{topic_table}";
        
        string sql_string = top_string + @" where (lower(original_value) = '' 
                        or lower(original_value) = 'human'
                        or lower(original_value) = 'humans'
                        or lower(original_value) = 'other'
                        or lower(original_value) = 'studies'
                        or lower(original_value) = 'evaluation'    
                        or lower(original_value) = 'n/a') ";
        delete_topics(topic_table, sql_string, "A", min_id, max_id, rec_batch, code_all);

        sql_string = top_string + @" where (lower(original_value) = 'healthy adults' 
                        or lower(original_value) = 'healthy adult'
                        or lower(original_value) = 'healthy person'
                        or lower(original_value) = 'healthy people'
                        or lower(original_value) = 'female'
                        or lower(original_value) = 'male'
                        or lower(original_value) = 'healthy adult female'
                        or lower(original_value) = 'healthy adult male') ";
        delete_topics(topic_table, sql_string, "B", min_id, max_id, rec_batch, code_all);

        sql_string = top_string + @" where (lower(original_value) = 'hv' 
                        or lower(original_value) = 'healthy volunteer'
                        or lower(original_value) = 'healthy volunteers'
                        or lower(original_value) = 'volunteer'
                        or lower(original_value) = 'healthy control'
                        or lower(original_value) = 'normal control') ";
        delete_topics(topic_table, sql_string, "C", min_id, max_id, rec_batch, code_all);

        sql_string = top_string + @" where (lower(original_value) = 'healthy individual' 
                        or lower(original_value) = 'healthy individuals'
                        or lower(original_value) = 'n/a(healthy adults)'
                        or lower(original_value) = 'n/a (healthy adults)'
                        or lower(original_value) = 'none (healthy adults)'
                        or lower(original_value) = 'healthy older adults'
                        or lower(original_value) = 'healthy japanese subjects') ";
        delete_topics(topic_table, sql_string, "D", min_id, max_id, rec_batch, code_all);

        sql_string = top_string + @" where (lower(original_value) = 'intervention' 
                        or lower(original_value) = 'implementation'
                        or lower(original_value) = 'prediction'
                        or lower(original_value) = 'recovery'
                        or lower(original_value) = 'healthy'
                        or lower(original_value) = 'complications') ";
        delete_topics(topic_table, sql_string, "E", min_id, max_id, rec_batch, code_all);

        sql_string = top_string + @" where (lower(original_value) = 'process evaluation' 
                        or lower(original_value) = 'follow-up'
                        or lower(original_value) = 'validation'
                        or lower(original_value) = 'tolerability'
                        or lower(original_value) = 'training'
                        or lower(original_value) = 'refractory') ";
        delete_topics(topic_table, sql_string, "F", min_id, max_id, rec_batch, code_all);

        sql_string = top_string + @" where (lower(original_value) = 'symptoms' 
                        or lower(original_value) = 'clinical research/ practice'
                        or lower(original_value) = 'predictors'
                        or lower(original_value) = 'management'
                        or lower(original_value) = 'disease'
                        or lower(original_value) = 'relapsed') ";
        delete_topics(topic_table, sql_string, "G", min_id, max_id, rec_batch, code_all);

        sql_string = top_string + @" where (lower(original_value) = 'complication' 
                        or lower(original_value) = '-'
                        or lower(original_value) = 'prep'
                        or lower(original_value) = 'not applicable'
                        or lower(original_value) = 'function'
                        or lower(original_value) = 'toxicity' 
                        or lower(original_value) = 'health condition 1: o- medical and surgical') ";
        delete_topics(topic_table, sql_string, "H", min_id, max_id, rec_batch, code_all);
    }


    public void delete_topics(string topic_table, string sql_string, string delete_set, 
                              int min_id, int max_id, int rec_batch, bool code_all)
    {
        // Normally only applies to newly added topic records or records that have not been coded in the past
        // (coded_on = null). Previous topic records will already have been filtered by this process.
        
        sql_string += code_all ? "" : " and coded_on is null ";
        
        string table = topic_table == "study_topics" ? "study": "object";
        string feedback_core = $"'no information' {table} topics (group {delete_set}) -";
        string qualifier = code_all ? "in all topics" : "in uncoded topics"; 
        try
        {
            if (max_id - min_id > rec_batch)
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " and id >= " + r + " and id < " + (r + rec_batch);
                    int res_r = ExecuteSQL(batch_sql_string);
                    if (res_r > 0)
                    {
                        int e =  r + rec_batch < max_id ? r + rec_batch : max_id;
                        _loggingHelper.LogLine($"Deleting {res_r} {feedback_core} {qualifier}, ids {r} to {e}");
                    }
                }
            }
            else
            {
                int res =  ExecuteSQL(sql_string);
                if (res > 0)
                {
                    _loggingHelper.LogLine($"Deleting {res} {feedback_core} {qualifier}, as a single query");
                }
            }
        }
        catch (Exception e)
        {
            string eres = e.Message;
            _loggingHelper.LogError("In deleting 'no information' categories: " + eres);
        }
    }


    public void identify_geographic(string topic_table, string schema, int min_id, int max_id, 
                                    int rec_batch, bool code_all)
    {
        // Normally only applies to newly added topic records or records that have not been coded in the past.
        // Previous topic records will already have been filtered by this process.
        
        string sql_string = $@"update {schema}.{topic_table} t 
                               set topic_type_id = 16
                               from context_ctx.country_names g
                               where t.original_value = g.alt_name ";
        sql_string += code_all ? "" : " and coded_on is null "; 
        
        string table = topic_table == "study_topics" ? "study": "object";
        string feedback_core = $"geographic {table} topics -";
        string qualifier = code_all ? "for all topics" : "for uncoded topics"; 
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
                        _loggingHelper.LogLine($"Identifying {res_r} {feedback_core} {qualifier} in ids {r} to {e}");
                    }
                }
            }
            else
            {
                int res = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Identifying {res} {feedback_core} {qualifier} as a single query");
            }
        }
        catch (Exception e)
        {
            string eres = e.Message;
            _loggingHelper.LogError("In identifying geographic topics: " + eres);
        }
    }


    public void mesh_match_topics(string topic_table, string schema, 
                                  int min_id, int max_id, int rec_batch, bool code_all)
    {
        string sql_string = $@"Update {schema}.{topic_table} t 
                               set mesh_code = m.code,
                               mesh_value = m.term,
                               coded_on = CURRENT_TIMESTAMP
                               from context_ctx.mesh_lookup m
                               where lower(t.original_value) = m.entry ";
        sql_string += code_all ? "" : " and coded_on is null ";
        
        string table = topic_table == "study_topics" ? "study": "object";
        string feedback_core = $"{table} topic codes ";
        string qualifier = code_all ? "for all topics" : "for uncoded topics"; 
        try
        {
            if (max_id - min_id > rec_batch)
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " and id >= " + r + " and id < " + (r + rec_batch);
                    int res_r = ExecuteSQL(batch_sql_string);
                    int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                    string feedback = $"Updating {res_r} {feedback_core} {qualifier}, ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                int res = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Updating {res} {feedback_core} {qualifier}, as a single query");
            }
        }
        catch (Exception e)
        {
            string eres = e.Message;
            _loggingHelper.LogError("In updating topics: " + eres);
        }
    }
    

    public void mesh_delete_duplicates(string topic_table, string schema, int min_id, int max_id, 
                                         int rec_batch, bool code_all)
    {
        // Changing to MESH codes may result in duplicate MESH terms (2 or more).
        // All but one of the duplicates should be removed.
        // The sql statements required are defined at the top of the procedure to make the processes
        // themselves much more concise and easier to understand.
        
        // The two sql clauses below will be used to create a temp table that holds the sd_sid / sd_oid,
        // mesh value and count of all the duplicated study / object - mesh combinations.
        
        string id_field = topic_table == "study_topics" ? "sd_sid": "sd_oid";
        string top_sql = $@"drop table if exists {schema}.temp_topic_dups;
                         create table {schema}.temp_topic_dups as 
                         select t.{id_field}, t.mesh_value, count(t.id) 
                         from {schema}.{topic_table} t ";
        if (!code_all)
        {
            top_sql += " inner join ad.uncoded_topic_records r on t.id = r.id ";
        }
        top_sql += " where mesh_value is not null ";  
        
        string grouping_sql =  $@" group by t.{id_field}, t.mesh_value 
                               having count(t.id) > 1";
        
        // The first statement below creates a table that has fields from all the records in the topics table
        // involved in duplications, while the second creates a table with the minimum id records for all the  
        // duplicated sets. N.B. When 'chunking' is in operation both statements require additional clauses.
       
        string make_table1_sql = $@"drop table if exists {schema}.all_duplicated_topic_ids;
                                  create table {schema}.all_duplicated_topic_ids
                                  as
                                  select t.{id_field}, t.mesh_value, t.id from  
                                  {schema}.{topic_table} t inner join {schema}.temp_topic_dups d
                                  on t.{id_field} = d.{id_field}
                                  and t.mesh_value = d.mesh_value ";
  
        string make_table2_sql = $@"drop table if exists {schema}.min_duplicated_topic_ids;
                                  create table {schema}.min_duplicated_topic_ids
                                  as 
                                  select t.{id_field}, t.mesh_value, min(t.id) as min_id from 
                                  {schema}.{topic_table} t inner join {schema}.temp_topic_dups d
                                  on t.{id_field} = d.{id_field}
                                  and t.mesh_value = d.mesh_value ";
        
        // The first statement below generates another temp table, with the ids of the records to be deleted,
        // by left joining the first table against the second. The second statement carries out the deletion.
        
        string make_table3_sql = $@"drop table if exists {schema}.topic_ids_to_delete;
                                create table {schema}.topic_ids_to_delete
                                as
                                select ax.id from {schema}.all_duplicated_topic_ids a
                                LEFT JOIN {schema}.min_duplicated_topic_ids m                                          
                                on x.{id_field} = m.{id_field}
                                on x.mesh_value = m.mesh_value
                                and a.id = m.min_id
                                where m.min_id is null; ";
        
        string delete_sql = $@"delete from {schema}.{topic_table} t
                           using {schema}.topic_ids_to_delete d
                           where t.id = d.id;";
        
        string table = topic_table == "study_topics" ? "study": "object";
        string feedback_core = $"duplicates in {table} topic codes -";
        string qualifier = code_all ? "for all topics" : "for uncoded topics";
        try
        {
            string sql_string;
            if (max_id - min_id > rec_batch)
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string id_qualifier = $" and t.id >= {r} and t.id < {r + rec_batch} ";
                    sql_string = top_sql + id_qualifier + grouping_sql;
                    ExecuteSQL(sql_string);
                    int res_r = GetTableCount(schema, "temp_topic_dups");
                    if (res_r > 0)
                    {
                        int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                        _loggingHelper.LogLine($"Identifying {res_r} {feedback_core} {qualifier}, ids {r} to {e}");
                        make_table1_sql += $" where t.id >= {r} and t.id < {e} ";
                        ExecuteSQL(make_table1_sql);  // creates table with all topic records involved in duplicates
                        make_table2_sql += @$" where t.id >= {r} and t.id < {e} 
                                              group by t.{id_field}, t.mesh_value ";
                        ExecuteSQL(make_table2_sql);  // creates table with min id topic records involved in duplicates
                        ExecuteSQL(make_table3_sql);  // creates the table wih the ids of the records to duplicate
                        int res_b = ExecuteSQL(delete_sql);        // Does the deletion
                        _loggingHelper.LogLine($"Deleted {res_b} {feedback_core} {qualifier}, ids {r} to {e}");
                    }
                }
            }
            else
            {
                sql_string = top_sql + grouping_sql;
                ExecuteSQL(sql_string);
                int res = GetTableCount(schema, "temp_topic_dups");
                if (res > 0)
                {
                    _loggingHelper.LogLine($"Identifying {res} {feedback_core} {qualifier}, as a single query");
                    ExecuteSQL(make_table1_sql);  // creates table with all topic records involved in duplicates
                    make_table2_sql += @$" group by t.{id_field}, t.mesh_value ";
                    ExecuteSQL(make_table2_sql);  // creates table with min id topic records involved in duplicates
                    ExecuteSQL(make_table3_sql);  // creates the table wih the ids of the records to duplicate
                    int res_a = ExecuteSQL(delete_sql);        // Does the deletion
                    _loggingHelper.LogLine($"Deleted {res_a} {feedback_core} {qualifier}, as a single query");
                }
            }

            // tidy up the temp tables

             sql_string = $@"drop table if exists {schema}.temp_topic_dups;
             drop table if exists {schema}.all_duplicated_topic_ids;
             drop table if exists {schema}.min_duplicated_topic_ids;
             drop table if exists {schema}.topic_ids_to_delete;";
             ExecuteSQL(sql_string);
        }
        catch (Exception e)
        {
            string eres = e.Message;
            _loggingHelper.LogError("In remove duplicate topics: " + eres);
        }
    }
 
   
    public int store_unmatched_topic_values(string source_type, int source_id)
    {
        string sql_string = @"delete from context_ctx.to_match_topics where source_id = " + source_id ;
        ExecuteSQL(sql_string);
        sql_string = @"insert into context_ctx.to_match_topics (source_id, topic_value, number_of) 
                   select " + source_id + @", original_value, count(original_value)";
        sql_string += source_type.ToLower() == "study"
                            ? " from ad.study_topics t"
                            : " from ad.object_topics t";
        sql_string += @" where t.mesh_code is null 
                         group by t.original_value;";
        int res = ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"Storing {res} topic codes not matched to MESH codes for review");
        return res;
    }
    
    public void delete_temp_tables()
    {
        string sql_string = @"drop table if exists ad.temp_topic_dups;
             drop table if exists ad.topic_ids_to_delete;
             drop table if exists ad.uncoded_study_topic_records; 
             drop table if exists ad.uncoded_object_topic_records ";
        ExecuteSQL(sql_string);
    }
}
