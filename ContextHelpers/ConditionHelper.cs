using Dapper;
using Npgsql;

namespace MDR_Coder
{ 
    public class ConditionHelper
    {
        private readonly string _db_conn;
        private readonly string _source_type;
        private readonly ILoggingHelper _loggingHelper;        
        
        private int study_rec_count;

        public ConditionHelper(Source source, ILoggingHelper logger)
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

        private void FeedbackConditionResults(string schema, string table_name, string id_field)
                   
        {
            int table_count = GetTableCount(schema, table_name);
            int org_id_count = GetFieldCount(schema, table_name, id_field);
            _loggingHelper.LogLine($"{org_id_count} records, from {table_count}, have ICD coded conditions in {schema}.{table_name}");
        }
       

        public void process_conditions(bool code_all)
        {
            string schema = _source_type == "test" ? "expected" : "ad";
       
            study_rec_count = GetTableCount(schema, "study_conditions");
            

            if (schema == "ad" && !code_all)
            {
                // keep a temp table record of condition record ids that are not yet coded

                string sql_string = @"drop table if exists ad.uncoded_study_condition_records;
                                create table ad.uncoded_study_condition_records (id INT primary key); 
                                insert into ad.uncoded_study_condition_records(id)
                                select id from ad.study_conditions where coded_on is null; ";

                using var conn = new NpgsqlConnection(_db_conn);
                conn.Execute(sql_string);
            }
            
            identify_no_info_conditions(schema, study_rec_count);       
            match_conditions_using_code(schema, study_rec_count, code_all);           
            match_conditions_using_term(schema, study_rec_count, code_all);
            resolve_multiple_condition_entries(schema, study_rec_count, code_all);
        }
        
        public void identify_no_info_conditions(string schema, int rec_count)
        {
            // Only applies to newly added condition records (coded_on = null). 
            // Previous condition records will already have been filtered by this process
            
            string top_string = $@"delete from {schema}.study_conditions "  ;

            string sql_string = top_string + @" where (lower(original_value) = '' 
                            or lower(original_value) = 'human'
                            or lower(original_value) = 'humans'
                            or lower(original_value) = 'other') ";
            delete_no_info_conditions(sql_string, rec_count, "A");

            sql_string = top_string + @" where (lower(original_value) = 'healthy adults' 
                            or lower(original_value) = 'healthy adult'
                            or lower(original_value) = 'healthy person'
                            or lower(original_value) = 'healthy people'
                            or lower(original_value) = 'female'
                            or lower(original_value) = 'male'
                            or lower(original_value) = 'healthy adult female'
                            or lower(original_value) = 'healthy adult male') ";
            delete_no_info_conditions(sql_string, rec_count, "B");

            sql_string = top_string + @" where (lower(original_value) = 'hv' 
                            or lower(original_value) = 'healthy volunteer'
                            or lower(original_value) = 'healthy volunteers'
                            or lower(original_value) = 'volunteer'
                            or lower(original_value) = 'healthy control'
                            or lower(original_value) = 'normal control') ";
            delete_no_info_conditions(sql_string, rec_count, "C");

            sql_string = top_string + @" where (lower(original_value) = 'healthy individual' 
                            or lower(original_value) = 'healthy individuals'
                            or lower(original_value) = 'n/a(healthy adults)'
                            or lower(original_value) = 'n/a (healthy adults)'
                            or lower(original_value) = 'none (healthy adults)'
                            or lower(original_value) = 'healthy older adults'
                            or lower(original_value) = 'healthy japanese subjects'
                            or lower(original_value) = 'toxicity' 
                            or lower(original_value) = 'health condition 1: o- medical and surgical') ";
            delete_no_info_conditions(sql_string, rec_count, "D");
        }


        public void delete_no_info_conditions(string sql_string, int rec_count, string delete_set)
        {
            // Can be difficult to do this with large datasets of conditions.
            
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
                        string feedback = $"Deleting {res_r} 'no information' conditions (group {delete_set}) - {r} to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch).ToString() : rec_count.ToString();
                        _loggingHelper.LogLine(feedback);
                    }
                }
                else
                {
                    int res = ExecuteSQL(sql_string);
                    _loggingHelper.LogLine($"Deleting {res} 'no information' conditions (group {delete_set}) - as a single query");
                }
                
            }
            catch (Exception e)
            {
                string eres = e.Message;
                _loggingHelper.LogError("In deleting 'no information' conditions: " + eres);
            }
        }
        
        public void match_conditions_using_code(string schema, int rec_count, bool code_all)
        {
            int rec_batch = 200000;
            string sql_string = @"Update " + schema + @".study_conditions t
                             set icd_code = m.icd_code,
                             icd_name = m.icd_term,
                             coded_on = CURRENT_TIMESTAMP
                             from context_ctx.icd_lookup m
                             where t.original_ct_code  = m.entry_code 
                             and t.original_ct_type_id = m.entry_type_id ";
            sql_string += code_all ? "" : " and coded_on is null ";
            try
            {
                if (rec_count > rec_batch)      // Can be difficult to do ths with large datasets.
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " and id >= " + r + " and id < " + (r + rec_batch);
                        int res_r = ExecuteSQL(batch_sql_string);
                        string feedback = $"Updating {res_r} study condition codes, using codes - {r} to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch).ToString() : rec_count.ToString();
                        _loggingHelper.LogLine(feedback);
                    }
                }
                else
                {
                    int res = ExecuteSQL(sql_string);
                    _loggingHelper.LogLine($"Updating {res} study condition codes, using codes -  as a single query");
                }
            }
            catch (Exception e)
            {
                string eres = e.Message;
                _loggingHelper.LogError("In updating conditions using codes: " + eres);
            }
        }

        
        public void match_conditions_using_term(string schema, int rec_count, bool code_all)
        {
            int rec_batch = 200000;
            string sql_string = @"Update " + schema + @".study_conditions t
                             set icd_code = m.icd_code,
                             icd_name = m.icd_term,
                             coded_on = CURRENT_TIMESTAMP
                             from context_ctx.icd_lookup m
                             where lower(t.original_value) = m.entry_lower ";
            sql_string += code_all ? "" : " and coded_on is null ";
            try
            {
                if (rec_count > rec_batch)   // Can be difficult to do ths with large datasets.
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " and id >= " + r + " and id < " + (r + rec_batch);
                        int res_r = ExecuteSQL(batch_sql_string);
                        string feedback = $"Updating {res_r} study condition codes, using terms - {r} to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch).ToString() : rec_count.ToString();
                        _loggingHelper.LogLine(feedback);
                    }
                }
                else
                {
                    int res = ExecuteSQL(sql_string);
                    _loggingHelper.LogLine($"Updating {res} study condition codes, using terms - as a single query");
                }

                FeedbackConditionResults(schema, "study_conditions", "icd_code");
            }
            catch (Exception e)
            {
                string eres = e.Message;
                _loggingHelper.LogError("In updating conditions using terms: " + eres);
            }
        }


        public void resolve_multiple_condition_entries(string schema, int rec_count, bool code_all)
        {
            // Consider only those with '//' in the term or code box
            // Should all be in the most recently added / coded set onl
            // Bring them out as a temp table and split on the //, then
            // split into separate lines.
            
            string sql_string = $@"drop table if exists {schema}.temp_mult_conds;
            create table {schema}.temp_mult_conds as
            select c.* from ad.study_conditions c ";
            if (schema == "ad" && !code_all)
            {
                sql_string += @"inner join ad.uncoded_study_condition_records u
                                on c.id = u.id ";
            }
            sql_string += " where c.icd_code like '%//%'";
            ExecuteSQL(sql_string);
            
            sql_string = $@"delete from {schema}.study_conditions sc
            using {schema}.temp_mult_conds mc
            where sc.id = mc.id ";
            ExecuteSQL(sql_string);  
            
            // Remove the '//' lines from the study_conditions table and replace it with the 
            // temporary table's contents.     
            
            sql_string = $@"insert into {schema}.study_conditions
            (sd_sid, original_value, original_ct_type_id, original_ct_code,
		    icd_code, icd_name, added_on, coded_on)
            select c.sd_sid, c.original_value, 
            c.original_ct_type_id, c.original_ct_code,
		    trim(t.icd_code), trim(t.icd_name), c.added_on, c.coded_on
            from {schema}.temp_mult_conds c
            cross join unnest(string_to_array(c.icd_code, '//'), string_to_array(c.icd_name, '//') ) 
            as t(icd_code, icd_name) ";
            ExecuteSQL(sql_string);  

           
            // Delete the temporary table.
            sql_string = $"drop table if exists {schema}.temp_mult_conds; ";
            ExecuteSQL(sql_string);
            
            // log revised number of records
            int new_count = GetTableCount(schema, "study_conditions");
            _loggingHelper.LogLine(
                $"study_conditions table has {new_count} records, after splitting of compound conditions");
        }

        public int store_unmatched_condition_values(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_conditions where source_id = source_id";
            ExecuteSQL(sql_string);
            sql_string = @"insert into context_ctx.to_match_conditions (source_id, condition_value, number_of) 
                           select " + source_id + @", original_value, count(original_value) 
                           from ad.study_conditions t 
                           where t.icd_code is null 
                           group by t.original_value;";
            int res = ExecuteSQL(sql_string);
            _loggingHelper.LogLine($"Storing {res} condition codes not matched to ICD codes for review");
            return res;
        }
        
        
        public void delete_temp_tables()
        {
            string sql_string = @"drop table if exists ad.uncoded_study_condition_records;";
            ExecuteSQL(sql_string);
        }
    }
}
