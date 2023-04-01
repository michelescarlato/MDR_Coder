using Dapper;
using Npgsql;

namespace MDR_Coder
{ 
    public class TopicHelper
    {
        private readonly Source _source;
        private readonly string _db_conn;
        private readonly bool _nonCodedOnly;
        private readonly string _source_type;
        private readonly ILoggingHelper _loggingHelper;        
        
        private int study_rec_count = 0;
        private int object_rec_count = 0;

        public TopicHelper(Source source, bool nonCodedOnly, ILoggingHelper logger)
        {
            _source = source;
            _db_conn = source.db_conn ?? "";
            _nonCodedOnly = nonCodedOnly;
            _source_type = source.source_type ?? "";
            _loggingHelper = logger;
        }

        public void ExecuteSQL(string sql_string)
        {
            using (var conn = new NpgsqlConnection(_db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        private int GetTopicCount(string source_type, string schema)
        {
            string sql_count_string = @"select count(*) from " + schema + ".";
            sql_count_string += source_type == "study" ? "study_topics;" : "object_topics;";

            using var conn = new NpgsqlConnection(_db_conn);
            return conn.ExecuteScalar<int>(sql_count_string);
        }
        
        private int GetConditionCount(string schema)
        {
            string sql_count_string = @"select count(*) from " + schema + ".study_conditions";
            using var conn = new NpgsqlConnection(_db_conn);
            return conn.ExecuteScalar<int>(sql_count_string);
        }


        // delete humans as subjects - as clinical research on humans...
        public void process_topics()
        {
            if (_source_type == "test")
            {
                study_rec_count = GetTopicCount("study", "expected");
                object_rec_count = GetTopicCount("object", "expected");

                delete_meaningless_cats("study", "expected", study_rec_count);
                delete_meaningless_cats("object", "expected", object_rec_count);

                identify_geographic("study", "expected", study_rec_count);
                identify_geographic("object", "expected", object_rec_count);
                
                mesh_match_topics("study", "expected", study_rec_count);
                mesh_match_topics("object", "expected", object_rec_count);

                mesh_remove_duplicates("study", "expected", study_rec_count);
                mesh_remove_duplicates("object", "expected", object_rec_count);
            }
            else if (_source_type.ToLower() == "study" && _source.has_study_topics is true)
            {
                study_rec_count = GetTopicCount("study", "ad");

                delete_meaningless_cats("study", "sd", study_rec_count);
                identify_geographic("study", "sd", study_rec_count);
                mesh_match_topics("study", "sd", study_rec_count);
                mesh_remove_duplicates("study", "sd", study_rec_count);
            }
            else if (_source_type.ToLower() == "object")
            {
                object_rec_count = GetTopicCount("object", "ad");

                delete_meaningless_cats("object", "sd", object_rec_count);
                identify_geographic("object", "sd", object_rec_count);  
                mesh_match_topics("object", "sd", object_rec_count);
                mesh_remove_duplicates("object", "sd", object_rec_count);
            }
        }

        public void delete_meaningless_cats(string source_type, string schema, int rec_count)
        {
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
            delete_topics(sql_string, rec_count, "B1");

            sql_string = top_string + @" where (lower(original_value) = 'hv' 
                            or lower(original_value) = 'healthy volunteer'
                            or lower(original_value) = 'healthy volunteers'
                            or lower(original_value) = 'volunteer'
                            or lower(original_value) = 'healthy control'
                            or lower(original_value) = 'normal control') ";
            delete_topics(sql_string, rec_count, "B2");

            sql_string = top_string + @" where (lower(original_value) = 'healthy individual' 
                            or lower(original_value) = 'healthy individuals'
                            or lower(original_value) = 'n/a(healthy adults)'
                            or lower(original_value) = 'n/a (healthy adults)'
                            or lower(original_value) = 'none (healthy adults)'
                            or lower(original_value) = 'healthy older adults'
                            or lower(original_value) = 'healthy japanese subjects') ";
            delete_topics(sql_string, rec_count, "C");

            sql_string = top_string + @" where (lower(original_value) = 'intervention' 
                            or lower(original_value) = 'implementation'
                            or lower(original_value) = 'prediction'
                            or lower(original_value) = 'recovery'
                            or lower(original_value) = 'healthy'
                            or lower(original_value) = 'complications') ";
            delete_topics(sql_string, rec_count, "D");

            sql_string = top_string + @" where (lower(original_value) = 'process evaluation' 
                            or lower(original_value) = 'follow-up'
                            or lower(original_value) = 'validation'
                            or lower(original_value) = 'tolerability'
                            or lower(original_value) = 'training'
                            or lower(original_value) = 'refractory') ";
            delete_topics(sql_string, rec_count, "E");

            sql_string = top_string + @" where (lower(original_value) = 'symptoms' 
                            or lower(original_value) = 'clinical research/ practice'
                            or lower(original_value) = 'predictors'
                            or lower(original_value) = 'management'
                            or lower(original_value) = 'disease'
                            or lower(original_value) = 'relapsed') ";
            delete_topics(sql_string, rec_count, "F");

            sql_string = top_string + @" where (lower(original_value) = 'complication' 
                            or lower(original_value) = '-'
                            or lower(original_value) = 'prep'
                            or lower(original_value) = 'not applicable'
                            or lower(original_value) = 'function'
                            or lower(original_value) = 'toxicity' 
                            or lower(original_value) = 'health condition 1: o- medical and surgical') ";
            delete_topics(sql_string, rec_count, "G");
        }


        public void delete_topics(string sql_string, int rec_count, string delete_set)
        {
            // Can be difficult to do ths with large datasets of topics.
            int rec_batch = 500000;

            try
            {
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " and id >= " + r.ToString() + " and id < " + (r + rec_batch).ToString();
                        ExecuteSQL(batch_sql_string);
                        string feedback = "Deleting meaningless topics (group " + delete_set + ") - " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch).ToString() : rec_count.ToString();
                        _loggingHelper.LogLine(feedback);
                    }
                }
                else
                {
                    ExecuteSQL(sql_string);
                    _loggingHelper.LogLine("Deleting meaningless topics (group " + delete_set + ") - as a single batch");
                }
            }
            catch (Exception e)
            {
                string res = e.Message;
                _loggingHelper.LogError("In deleting meaningless categories: " + res);
            }
        }


        public void identify_geographic(string source_type, string schema, int rec_count)
        {
            string sql_string = source_type == "study" 
                                ? "update " + schema + ".study_topics t "
                                : "update " + schema + ".object_topics t ";
            sql_string += @"set topic_type_id = 16
                                  from context_ctx.geog_entities g
                                  where t.original_value = g.name
                                  and topic_type_id is null ";
  
            // Can be difficult to do ths with large datasets of topics.
            int rec_batch = 500000;

            try
            {
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " and t.id >= " + r.ToString() + " and t.id < " + (r + rec_batch).ToString();
                        ExecuteSQL(batch_sql_string);
                        string feedback = "Identifying geographic topics - " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch).ToString() : rec_count.ToString();
                        _loggingHelper.LogLine(feedback);
                    }
                }
                else
                {
                    ExecuteSQL(sql_string);
                    _loggingHelper.LogLine("Identifying geographic topics - as a single batch");
                }
            }
            catch (Exception e)
            {
                string res = e.Message;
                _loggingHelper.LogError("In identifying geographic topics: " + res);
            }
        }


        public void mesh_match_topics(string source_type, string schema, int rec_count)
        {
            // Can be difficult to do ths with large datasets.
            int rec_batch = 500000;

            // In some cases mesh codes may be overwritten if 
            // they do not conform entirely (in format) with the mesh list.

            string sql_string = @"Update " + schema;
            sql_string += source_type == "study"  
                                ? ".study_topics t "
                                : ".object_topics t ";
            sql_string += @" set mesh_code = m.code,
                             mesh_value = m.term,
                             from context_ctx.mesh_lookup m
                             where lower(t.original_value) = m.entry";
            try
            {
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " and id >= " + r.ToString() + " and id < " + (r + rec_batch).ToString();
                        ExecuteSQL(batch_sql_string);
                        string feedback = "Updating " + source_type + " topic codes - " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch).ToString() : rec_count.ToString();
                        _loggingHelper.LogLine(feedback);
                    }
                }
                else
                {
                    ExecuteSQL(sql_string);
                    _loggingHelper.LogLine("Updating " + source_type + " topic codes - as a single batch");
                }
            }
            catch (Exception e)
            {
                string res = e.Message;
                _loggingHelper.LogError("In update_topics: " + res);
            }
        }

        
        public void icd_match_conditions(string source_type, string schema, int rec_count)
        {
            // Can be difficult to do ths with large datasets.
            int rec_batch = 500000;

            string sql_string = @"Update " + schema + @".study_conditions t
                             set icd_code = m.code,
                             icd_name = m.term,
                             from context_ctx.icd_lookup m
                             where lower(t.original_value) = m.entry ";
            try
            {
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " and id >= " + r.ToString() + " and id < " + (r + rec_batch).ToString();
                        ExecuteSQL(batch_sql_string);
                        string feedback = "Updating " + source_type + " topic codes - " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch).ToString() : rec_count.ToString();
                        _loggingHelper.LogLine(feedback);
                    }
                }
                else
                {
                    ExecuteSQL(sql_string);
                    _loggingHelper.LogLine("Updating " + source_type + " topic codes - as a single batch");
                }
            }
            catch (Exception e)
            {
                string res = e.Message;
                _loggingHelper.LogError("In update_topics: " + res);
            }
        }

        public void mesh_remove_duplicates(string source_type, string schema, int rec_count)
        {
            // Changing to MESH codes may result in duplicate MESH terms, 
            // one of them needs to be removed...
            // Can be difficult to do ths with large datasets.
            int rec_batch = 100000;
            string sql_string;
            string id_field = source_type == "study"? "sd_sid": "sd_oid";
            string topics_table = source_type == "study" ? "study_topics" : "object_topics";

            try
            {
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        sql_string = @"drop table if exists " + schema + @".temp_topic_dups;
                              create table " + schema + @".temp_topic_dups
                              as
                              select " + id_field + ", mesh_value, count(id) from " +
                              schema + "." + topics_table + @" t where mesh_coded = true 
                              and id >= " + r.ToString() + " and id < " + (r + rec_batch).ToString() + @"
                              group by " + id_field + @", mesh_value 
                              having count(id) > 1";

                        ExecuteSQL(sql_string);

                        delete_duplicates(schema, id_field, topics_table);

                        string feedback = "Deleting duplicates in " + source_type + " topic codes - " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch).ToString() : rec_count.ToString();
                        _loggingHelper.LogLine(feedback);
                    }
                }
                else
                {
                    sql_string = @"drop table if exists " + schema + @".temp_topic_dups;
                               create table " + schema + @".temp_topic_dups
                               as
                               select " + id_field + ", mesh_value, count(id) from " +
                               schema + "." + topics_table + @" t where mesh_coded = true 
                               group by " + id_field + @", mesh_value 
                               having count(id) > 1;";

                    ExecuteSQL(sql_string);

                    delete_duplicates(schema, id_field, topics_table);

                    _loggingHelper.LogLine("Deleting duplicates in " + source_type + " topic codes - as a single batch");
                }

                // tidy up the temp tables

                sql_string = @"drop table if exists sd.temp_topic_dups;
                  drop table if exists sd.topic_ids_to_delete;";
            }
            catch (Exception e)
            {
                string res = e.Message;
                _loggingHelper.LogError("In remove duplicate topics: " + res);
            }
        }


        private void delete_duplicates(string schema, string id_field, string topics_table)
        {
            // table sd.temp_topic_dups has been created by the calling function
            // the SQL below uses the duplicated values to identify the records to
            // delete the 'non-minimum' ones of each duplicated set.

            string sql_string = @"drop table if exists sd.topic_ids_to_delete;
                             create table sd.topic_ids_to_delete
                             as
                             select ax.* from 
                                (select t." + id_field + @", t.mesh_value, t.id from " + 
                                 schema + "." + topics_table + @" t inner join " + schema + @".temp_topic_dups d
                                 on t." + id_field + @" = d." + id_field + @"
                                 and t.mesh_value = d.mesh_value) ax
                            LEFT JOIN
                                (select t." + id_field + @", t.mesh_value, min(t.id) as minid from " +
                                 schema + "." + topics_table + @" t inner join " + schema + @".temp_topic_dups d
                                 on t." + id_field + @" = d." + id_field + @"
                                 and t.mesh_value = d.mesh_value
                                 group by t." + id_field + @", t.mesh_value) b
                            on ax." + id_field + @" = b." + id_field + @"
                            and ax.id = b.minid
                            where b.minid is null
                            order by ax." + id_field + @", ax.mesh_value, ax.id;";

            ExecuteSQL(sql_string);

            sql_string = @"delete from sd." + topics_table + @" t
                               using sd.topic_ids_to_delete d
                               where t.id = d.id;";

            ExecuteSQL(sql_string);

        }

        public void process_conditions()
        {
            study_rec_count = GetConditionCount("ad");
            icd_match_conditions("study", "sd", study_rec_count);
            // icd_remove_duplicates("study", "sd", study_rec_count);  // not sure if required
        }

        public void store_unmatched_topic_values(string source_type, int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_topics where source_id = "
            + source_id.ToString() + @";
            insert into context_ctx.to_match_topics (source_id, topic_value, number_of) 
            select " + source_id.ToString() + @", original_value, count(original_value)";

            sql_string += source_type.ToLower() == "study"
                                ? " from sd.study_topics t"
                                : " from sd.object_topics t";
            sql_string += @" where t.mesh_code is null 
                             group by t.original_value;";
            ExecuteSQL(sql_string);
            _loggingHelper.LogLine("Storing topic codes not matched to MESH codes");
        }
        
        
        public void store_unmatched_condition_values(string source_type, int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_conditions where source_id = "
                                + source_id + @";
            insert into context_ctx.to_match_conditions (source_id, original_value, number_of) 
            select " + source_id + @", original_value, count(original_value) ";
            sql_string += @" from sd.study_conditions t 
                             where t.icd_code is null 
                             group by t.original_value;";
            ExecuteSQL(sql_string);
            _loggingHelper.LogLine("Storing condition codes not matched to ICD codes");
        }
    }
}
