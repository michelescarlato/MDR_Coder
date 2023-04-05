using System.Net.Http.Headers;
using Dapper;
using Npgsql;

namespace MDR_Coder
{ 
    public class TopicHelper
    {
        private readonly Source _source;
        private readonly string _db_conn;
        private readonly string _source_type;
        private readonly ILoggingHelper _loggingHelper;        
        
        private int study_rec_count;
        private int object_rec_count;

        public TopicHelper(Source source, ILoggingHelper logger)
        {
            _source = source;
            _db_conn = source.db_conn ?? "";
            _source_type = source.source_type ?? "";
            _loggingHelper = logger;
        }

        public int ExecuteSQL(string sql_string)
        {
            using var conn = new NpgsqlConnection(_db_conn);
            return conn.Execute(sql_string);
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
            int org_id_count = GetFieldCount(schema, table_name, id_field);
            _loggingHelper.LogLine($"{org_id_count} records, from {table_count}, have MESH coded topics in {schema}.{table_name}");
        }

        public void process_topics(bool code_all)
        {
            string schema = "";
            string target_table  = "";
            if (_source_type == "test")
            {
                // For a test, i.e. 'expected' schema data, code everything each time.
                
                schema = "expected";
                target_table = "study_topics";
                
                study_rec_count = GetTableCount(schema, target_table);
                delete_no_information_cats("study", schema, study_rec_count);
                identify_geographic("study", schema, study_rec_count);                
                mesh_match_topics("study", schema, study_rec_count, true);       
                mesh_identify_duplicates("study", schema, study_rec_count, true);         
                FeedbackTopicResults(schema, target_table, "mesh_code");
                
                target_table = "object_topics";
                object_rec_count = GetTableCount(schema, target_table);                
                delete_no_information_cats("object", schema, object_rec_count);
                identify_geographic("object", schema, object_rec_count);
                mesh_match_topics("object", schema, object_rec_count, true);         
                mesh_identify_duplicates("object", schema, object_rec_count, true);        
                FeedbackTopicResults(schema, target_table, "mesh_code");
            }
            else
            {
                schema = "ad";
                if (_source_type.ToLower() == "study")
                {
                    target_table = "study_topics";
                    if (_source.has_study_topics is true)
                    {
                        if (!code_all)
                        {
                            // keep a temp table record of topic record ids that are not yet coded

                            string sql_string = @"drop table if exists ad.uncoded_study_topic_records;
                                create table ad.uncoded_study_topic_records (id INT primary key); 
                                insert into ad.uncoded_study_topic_records(id)
                                select id from ad.study_topics where coded_on is null; ";

                            using var conn = new NpgsqlConnection(_db_conn);
                            conn.Execute(sql_string);
                        }

                        study_rec_count = GetTableCount(schema, target_table);
                        delete_no_information_cats("study", schema, study_rec_count);
                        identify_geographic("study", schema, study_rec_count);
                        mesh_match_topics("study", schema, study_rec_count, code_all);
                        mesh_identify_duplicates("study", schema, study_rec_count, code_all);
                        FeedbackTopicResults(schema, target_table, "mesh_code");
                    }
                }
                else if (_source_type.ToLower() == "object")
                {
                    target_table = "object_topics";
                    if (!code_all) // keep a record of topic record ids not yet coded
                    {
                        // keep a temp table record of topic record ids that are not yet coded   

                        string sql_string = @"drop table if exists ad.uncoded_object_topic_records;          
                            create table ad.uncoded_object_topic_records (id INT primary key);                   
                            insert into ad.uncoded_object_topic_records(id)                                      
                            select id from ad.object_topics where coded_on is null; ";

                        using var conn = new NpgsqlConnection(_db_conn);
                        conn.Execute(sql_string);
                    }

                    object_rec_count = GetTableCount(schema, target_table);
                    delete_no_information_cats("object", schema, object_rec_count);
                    identify_geographic("object", schema, object_rec_count);
                    mesh_match_topics("object", schema, object_rec_count, code_all);
                    mesh_identify_duplicates("object", schema, object_rec_count, code_all);
                    FeedbackTopicResults(schema, target_table, "mesh_code");
                }
            }
        }

        
        public void delete_no_information_cats(string source_type, string schema, int rec_count)
        {
            // Only applies to newly added topic records or records that have not been coded in the past
            // (coded_on = null). Previous topic records will already have been filtered by this process
            
            string top_string = @"delete from " + schema;
            top_string += source_type == "study" ? ".study_topics " : ".object_topics ";
            
            string sql_string = top_string + @" where (lower(original_value) = '' 
                            or lower(original_value) = 'human'
                            or lower(original_value) = 'humans'
                            or lower(original_value) = 'other'
                            or lower(original_value) = 'studies'
                            or lower(original_value) = 'evaluation') ";
            delete_topics(sql_string, rec_count, "A");

            sql_string = top_string + @" where (lower(original_value) = 'healthy adults' 
                            or lower(original_value) = 'healthy adult'
                            or lower(original_value) = 'healthy person'
                            or lower(original_value) = 'healthy people'
                            or lower(original_value) = 'female'
                            or lower(original_value) = 'male'
                            or lower(original_value) = 'healthy adult female'
                            or lower(original_value) = 'healthy adult male') ";
            delete_topics(sql_string, rec_count, "B");

            sql_string = top_string + @" where (lower(original_value) = 'hv' 
                            or lower(original_value) = 'healthy volunteer'
                            or lower(original_value) = 'healthy volunteers'
                            or lower(original_value) = 'volunteer'
                            or lower(original_value) = 'healthy control'
                            or lower(original_value) = 'normal control') ";
            delete_topics(sql_string, rec_count, "C");

            sql_string = top_string + @" where (lower(original_value) = 'healthy individual' 
                            or lower(original_value) = 'healthy individuals'
                            or lower(original_value) = 'n/a(healthy adults)'
                            or lower(original_value) = 'n/a (healthy adults)'
                            or lower(original_value) = 'none (healthy adults)'
                            or lower(original_value) = 'healthy older adults'
                            or lower(original_value) = 'healthy japanese subjects') ";
            delete_topics(sql_string, rec_count, "D");

            sql_string = top_string + @" where (lower(original_value) = 'intervention' 
                            or lower(original_value) = 'implementation'
                            or lower(original_value) = 'prediction'
                            or lower(original_value) = 'recovery'
                            or lower(original_value) = 'healthy'
                            or lower(original_value) = 'complications') ";
            delete_topics(sql_string, rec_count, "E");

            sql_string = top_string + @" where (lower(original_value) = 'process evaluation' 
                            or lower(original_value) = 'follow-up'
                            or lower(original_value) = 'validation'
                            or lower(original_value) = 'tolerability'
                            or lower(original_value) = 'training'
                            or lower(original_value) = 'refractory') ";
            delete_topics(sql_string, rec_count, "F");

            sql_string = top_string + @" where (lower(original_value) = 'symptoms' 
                            or lower(original_value) = 'clinical research/ practice'
                            or lower(original_value) = 'predictors'
                            or lower(original_value) = 'management'
                            or lower(original_value) = 'disease'
                            or lower(original_value) = 'relapsed') ";
            delete_topics(sql_string, rec_count, "G");

            sql_string = top_string + @" where (lower(original_value) = 'complication' 
                            or lower(original_value) = '-'
                            or lower(original_value) = 'prep'
                            or lower(original_value) = 'not applicable'
                            or lower(original_value) = 'function'
                            or lower(original_value) = 'toxicity' 
                            or lower(original_value) = 'health condition 1: o- medical and surgical') ";
            delete_topics(sql_string, rec_count, "H");
        }


        public void delete_topics(string sql_string, int rec_count, string delete_set)
        {
            // Can be difficult to do this with large datasets of topics.
            
            sql_string += " and coded_on is null ";
            int rec_batch = 200000;                                                      

            try
            {
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " and id >= " + r + " and id < " + (r + rec_batch);
                        int res_r = ExecuteSQL(batch_sql_string);
                        string feedback = $"Deleting {res_r} 'no information' topics (group {delete_set}) - {r} to ";
                        feedback += r + rec_batch < rec_count ? (r + rec_batch).ToString() : rec_count.ToString();
                        _loggingHelper.LogLine(feedback);
                    }
                }
                else
                {
                    int res =  ExecuteSQL(sql_string);
                    _loggingHelper.LogLine($"Deleting {res} 'no information' topics (group {delete_set}) - as a single query");
                }
            }
            catch (Exception e)
            {
                string eres = e.Message;
                _loggingHelper.LogError("In deleting 'no information' categories: " + eres);
            }
        }


        public void identify_geographic(string source_type, string schema, int rec_count)
        {
            // Only applies to newly added topic records or records that have not been coded in the past
            // (coded_on = null). Previous topic records will already have been filtered by this process
            
            string sql_string = source_type == "study" 
                                ? "update " + schema + ".study_topics t "
                                : "update " + schema + ".object_topics t ";
            sql_string += @"set topic_type_id = 16
                                  from context_ctx.country_names g
                                  where t.original_value = g.alt_name
                                  and topic_type_id is null 
                                  and coded_on is null ";
  
            // Can be difficult to do ths with large datasets of topics.
            int rec_batch = 200000;

            try
            {
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " and t.id >= " + r + " and t.id < " + (r + rec_batch);
                        int res_r = ExecuteSQL(batch_sql_string);
                        string feedback = $"Identifying {res_r} geographic topics - {r} to ";
                        feedback += r + rec_batch < rec_count ? (r + rec_batch).ToString() : rec_count.ToString();
                        _loggingHelper.LogLine(feedback);
                    }
                }
                else
                {
                    int res = ExecuteSQL(sql_string);
                    _loggingHelper.LogLine($"Identifying {res} geographic topics - as a single query");
                }
            }
            catch (Exception e)
            {
                string eres = e.Message;
                _loggingHelper.LogError("In identifying geographic topics: " + eres);
            }
        }


        public void mesh_match_topics(string source_type, string schema, int rec_count, bool code_all)
        {
            int rec_batch = 200000;   // Can be difficult to do ths with large datasets.

            string sql_string = @"Update " + schema;
            sql_string += source_type == "study"  
                                ? ".study_topics t "
                                : ".object_topics t ";
            sql_string += @" set mesh_code = m.code,
                             mesh_value = m.term,
                             coded_on = CURRENT_TIMESTAMP
                             from context_ctx.mesh_lookup m
                             where lower(t.original_value) = m.entry ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            try
            {
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " and id >= " + r + " and id < " + (r + rec_batch);
                        int res_r = ExecuteSQL(batch_sql_string);
                        string feedback = $"Updating {res_r} {source_type} topic codes - {r} to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch).ToString() : rec_count.ToString();
                        _loggingHelper.LogLine(feedback);
                    }
                }
                else
                {
                    int res = ExecuteSQL(sql_string);
                    _loggingHelper.LogLine($"Updating {res} {source_type} topic codes - as a single query");
                }
            }
            catch (Exception e)
            {
                string eres = e.Message;
                _loggingHelper.LogError("In updating topics: " + eres);
            }
        }
        

        public void mesh_identify_duplicates(string source_type, string schema, int rec_count, bool code_all)
        {
            // Changing to MESH codes may result in duplicate MESH terms, 
            // one of them needs to be removed...   Can be difficult to do ths with large datasets.
            
            int rec_batch = 100000;
            string id_field = source_type == "study"? "sd_sid": "sd_oid";
            string topics_table = source_type == "study" ? "study_topics" : "object_topics";
            string added_records = source_type == "study" ? "ad.uncoded_study_topic_records" 
                                                          : "ad.uncoded_object_topic_records";

            string top_sql = $@"drop table if exists {schema}.temp_topic_dups;
                             create table {schema}.temp_topic_dups as 
                             select t.{id_field}, t.mesh_value, count(t.id) from {schema}.{topics_table} t ";
            if (_source_type != "test")
            {
                top_sql += $" inner join {added_records} r on t.id = r.id ";
            }
            top_sql += $" where mesh_value is not null ";           
            string grouping_sql =  $@" group by t.{id_field}, t.mesh_value 
                                   having count(t.id) > 1";
            try
            {
                string sql_string;
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        sql_string = top_sql +
                              $" and id >= {r} and id < {r + rec_batch} "
                              + grouping_sql;

                        ExecuteSQL(sql_string);
                        int res_r = GetTableCount(schema, "temp_topic_dups");
                        string feedback = $"Identifying {res_r} duplicates in {source_type} topic codes - {r} to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch).ToString() : rec_count.ToString();
                        _loggingHelper.LogLine(feedback);
                        delete_identified_duplicates(schema, id_field, topics_table, feedback);
                    }
                }
                else
                {
                    sql_string = top_sql + grouping_sql;
                    ExecuteSQL(sql_string);
                    int res = GetTableCount(schema, "temp_topic_dups");
                    string feedback = $"Identifying {res} duplicates in {source_type} topic codes - as a single query";
                    _loggingHelper.LogLine(feedback);
                    delete_identified_duplicates(schema, id_field, topics_table, feedback);      
                }

                // tidy up the temp tables

                 sql_string = @"drop table if exists ad.temp_topic_dups;
                 drop table if exists ad.topic_ids_to_delete;";
                 ExecuteSQL(sql_string);

                 
            }
            catch (Exception e)
            {
                string eres = e.Message;
                _loggingHelper.LogError("In remove duplicate topics: " + eres);
            }
        }


        private void delete_identified_duplicates(string schema, string id_field, string topics_table, string feedback)
        {
            // table ad.temp_topic_dups has been created by the calling function
            // the SQL below uses the duplicated values to identify the records to
            // delete the 'non-minimum' ones of each duplicated set.

            string fback = " " + feedback[feedback.IndexOf("duplicates", 0, StringComparison.Ordinal)..];
            string sql_string = $@"drop table if exists ad.topic_ids_to_delete;
                             create table ad.topic_ids_to_delete
                             as
                             select ax.* from 
                                (select t.{id_field}, t.mesh_value, t.id from  
                                 {schema}.{topics_table} t inner join {schema}.temp_topic_dups d
                                 on t.{id_field} = d.{id_field}
                                 and t.mesh_value = d.mesh_value) ax
                             LEFT JOIN
                                (select t.{id_field}, t.mesh_value, min(t.id) as min_id from 
                                 {schema}.{topics_table} t inner join {schema}.temp_topic_dups d
                                 on t.{id_field} = d.{id_field}
                                 and t.mesh_value = d.mesh_value
                                 group by t.{id_field}, t.mesh_value) b
                            on ax.{id_field} = b.{id_field}
                            and ax.id = b.min_id
                            where b.min_id is null; ";

            ExecuteSQL(sql_string);

            sql_string = $@"delete from {schema}.{topics_table} t
                               using {schema}.topic_ids_to_delete d
                               where t.id = d.id;";

            int res = ExecuteSQL(sql_string);
            _loggingHelper.LogLine("Deleting " + res + fback);

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
}
