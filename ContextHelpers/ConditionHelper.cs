using Dapper;
using Npgsql;

namespace MDR_Coder;

public class ConditionHelper
{
    private readonly string _db_conn;
    private readonly ILoggingHelper _loggingHelper; 
    private readonly string scope_qualifier;
    private readonly string feedback_qualifier;
    
    public ConditionHelper(Source source, ILoggingHelper logger, int scope, bool recodeTestDataOnly)
    {
        _db_conn = source.db_conn ?? "";
        _loggingHelper = logger;
        scope_qualifier = scope == 1 ? " and c.coded_on is null " : "";
        feedback_qualifier = scope == 1 ? "unmatched" : "all";
        if (recodeTestDataOnly)    // test data 'trumps' the decisions above 
        {
            scope_qualifier = " and c.sd_sid in (select sd_sid from mn.test_study_list) ";
            feedback_qualifier = "test data";
        }
    }

    public int ExecuteSQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.Execute(sql_string);
    }

    private int GetMinId(string table_name)
    {
        string sql_string = @$"select min(id) from ad.{table_name};";
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
    }
    
    private int GetMaxId(string table_name)
    {
        string sql_string = @$"select max(id) from ad.{table_name};";
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
    }
    
    private int GetTableCount(string table_name)
    {
        // gets the total numbers of records in the table (even if not all will be updated,    
        // they will all be included in the query).                                            
        
        string sql_string = $"select count(*) from ad.{table_name};";
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
    }
    
    private int GetFieldCount(string table_name, string field_name)
    {
        // gets the total numbers of records in the table (even if not all will be updated,    
        // they will all be included in the query).                                            
        
        string sql_string = @$"select count(*) from ad.{table_name}
                               where {field_name} is not null;";
        using var conn = new NpgsqlConnection(_db_conn);
        return conn.ExecuteScalar<int>(sql_string);
    }

    private void FeedbackConditionResults(string schema, string table_name, string id_field)
    {
        int table_count = GetTableCount(table_name);
        int coded_count = GetFieldCount(table_name, id_field);
        _loggingHelper.LogLine($"{coded_count} records, from {table_count}, " +
                               $"{(double) 100 * coded_count / table_count:N1} %, " +  
                               $"have ICD coded conditions in {schema}.{table_name}");
    }
   

    public void process_conditions()
    {
        int min_id = GetMinId("study_conditions");
        int max_id = GetMaxId("study_conditions");
       
        // Then code the condition data in these 4 steps.
        
        identify_no_info_conditions(min_id, max_id, 200000);       
        match_conditions_using_code(min_id, max_id, 200000);           
        match_conditions_using_term(min_id, max_id, 200000);
        resolve_multiple_condition_entries();
    }
    
    public void identify_no_info_conditions(int min_id, int max_id, int rec_batch)
    {
        // Only normally applies to newly added condition records (coded_on = null). 
        // Previous condition records will already have been filtered by this process
        
        string top_string = $@"delete from ad.study_conditions c "  ;

        string sql_string = top_string + $@" where (lower(original_value) = '' 
                        or lower(original_value) = 'human'
                        or lower(original_value) = 'humans'
                        or lower(original_value) = 'other'
                        or lower(original_value) = 'women'
                        or lower(original_value) = 'child'
                        or lower(original_value) = 'adolescent'
                        or lower(original_value) = 'adolescents'
                        or lower(original_value) = 'men') 
                        {scope_qualifier}";
        delete_no_info_conditions(sql_string, min_id, max_id, "A", rec_batch);

        sql_string = top_string + $@" where (lower(original_value) = 'healthy adults' 
                        or lower(original_value) = 'healthy adult'
                        or lower(original_value) = 'healthy person'
                             or lower(original_value) = 'healthy people'
                        or lower(original_value) = 'female'
                        or lower(original_value) = 'male'
                        or lower(original_value) = 'healthy adult female'
                        or lower(original_value) = 'healthy adult male') 
                        {scope_qualifier}";
        delete_no_info_conditions(sql_string, min_id, max_id, "B", rec_batch);

        sql_string = top_string + $@" where (lower(original_value) = 'hv' 
                        or lower(original_value) = 'healthy volunteer'
                        or lower(original_value) = 'healthy volunteers'
                        or lower(original_value) = 'volunteer'
                        or lower(original_value) = 'healthy control'
                        or lower(original_value) = 'normal control') 
                        {scope_qualifier}";
        delete_no_info_conditions(sql_string, min_id, max_id, "C", rec_batch);

        sql_string = top_string + $@" where (lower(original_value) = 'healthy individual' 
                        or lower(original_value) = 'healthy individuals'
                        or lower(original_value) = 'n/a(healthy adults)'
                        or lower(original_value) = 'n/a (healthy adults)'
                        or lower(original_value) = 'none (healthy adults)'
                        or lower(original_value) = 'healthy older adults'
                        or lower(original_value) = 'healthy japanese subjects'
                        or lower(original_value) = 'toxicity' 
                        or lower(original_value) = 'health condition 1: o- medical and surgical') 
                        {scope_qualifier}";
        delete_no_info_conditions(sql_string, min_id, max_id, "D", rec_batch);
        
        sql_string = top_string + $@" where (lower(original_value) = 'body weight' 
                        or lower(original_value) = 'disease'
                        or lower(original_value) = 'emergencies'
                        or lower(original_value) = 'healthy'
                        or lower(original_value) = 'inflammation'
                        or lower(original_value) = 'ischemia'
                        or lower(original_value) = 'sclerosis'
                        or lower(original_value) = 'thrombosis' 
                        or lower(original_value) = 'ulcer') 
                        {scope_qualifier}";
        delete_no_info_conditions(sql_string, min_id, max_id, "E", rec_batch);
        
        sql_string = top_string + $@" where (lower(original_value) = 'body weight' 
                        or lower(original_value) = 'surgery'
                        or lower(original_value) = 'syndrome'
                        or lower(original_value) = 'sleep'
                        or lower(original_value) = 'public health'
                        or lower(original_value) = 'public health - epidemiology'
                        or lower(original_value) = 'public health - health promotion/education'
                        or lower(original_value) = 'quality of life' 
                        or lower(original_value) = 'recurrence') 
                        {scope_qualifier}";
        delete_no_info_conditions(sql_string, min_id, max_id, "F", rec_batch);
        
        sql_string = top_string + $@" where (lower(original_value) = 'pharmacokinetic study' 
                        or lower(original_value) = 'pharmacokinetics and bioequivalence study in human'
                        or lower(original_value) = 'physical activity'
                        or lower(original_value) = 'physical function'
                        or lower(original_value) = 'physical inactivity'
                        or lower(original_value) = 'infarction'
                        or lower(original_value) = 'fibrosis'
                        or lower(original_value) = 'constriction, pathologic' 
                        or lower(original_value) = 'critical illness'
                        or lower(original_value) = 'critically ill patients' 
                        or lower(original_value) = 'chronic disease') 
                        {scope_qualifier}";
        delete_no_info_conditions(sql_string, min_id, max_id, "G", rec_batch);
    }

 
    public void delete_no_info_conditions(string sql_string, int min_id, int max_id, 
                                          string delete_set, int rec_batch)
    {
        // Can be difficult to do this with large datasets of conditions.
        string feedback_core = $"'no information' conditions (group {delete_set})";
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
                        int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                        _loggingHelper.LogLine($"Deleting {res_r} {feedback_core} in {feedback_qualifier} records - {r} to {e}");
                    }
                }
            }
            else
            {
                int res = ExecuteSQL(sql_string);
                if (res > 0)
                {
                    _loggingHelper.LogLine($"Deleting {res} {feedback_core} in {feedback_qualifier} records - as a single query");
                }
            }
        }
        catch (Exception e)
        {
            string eres = e.Message;
            _loggingHelper.LogError("In deleting 'no information' conditions: " + eres);
        }
    }
    
    public void match_conditions_using_code(int min_id, int max_id, int rec_batch)
    {
        string sql_string = $@"Update ad.study_conditions c
                         set icd_code = m.icd_code, icd_name = m.icd_term,
                         coded_on = CURRENT_TIMESTAMP
                         from context_ctx.icd_codes_lookup m
                         where c.original_ct_code  = m.entry_code 
                         and c.original_ct_type_id = m.entry_code_type_id  
                         {scope_qualifier}";
        string feedback_core = "study condition codes, using codes";
        try
        {
            if (max_id - min_id > rec_batch)      // Can be difficult to do ths with large datasets.
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " and t.id >= " + r + " and t.id < " + (r + rec_batch);
                    int res_r = ExecuteSQL(batch_sql_string);
                    int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                    _loggingHelper.LogLine($"Updating {res_r} {feedback_core} in {feedback_qualifier} records - {r} to {e}");
                }
            }
            else
            {
                int res = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Updating {res} {feedback_core} in {feedback_qualifier} records -  as a single query");
            }
        }
        catch (Exception e)
        {
            string eres = e.Message;
            _loggingHelper.LogError("In updating conditions using codes: " + eres);
        }
    }

    
    public void match_conditions_using_term(int min_id, int max_id, int rec_batch)
    {
        string sql_string = $@"Update ad.study_conditions c
                         set icd_code = m.icd_code, icd_name = m.icd_term,
                         coded_on = CURRENT_TIMESTAMP
                         from context_ctx.icd_terms_lookup m
                         where lower(c.original_value) = lower(m.entry_term)
                         {scope_qualifier}";
        string feedback_core = "study condition codes, using terms";
        try
        {
            if (max_id - min_id > rec_batch)   // Can be difficult to do ths with large datasets.
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " and t.id >= " + r + " and t.id < " + (r + rec_batch);
                    int res_r = ExecuteSQL(batch_sql_string);
                    int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                    _loggingHelper.LogLine($"Updating {res_r} {feedback_core} in {feedback_qualifier} records - {r} to {e}");
                }
            }
            else
            {
                int res = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Updating {res} {feedback_core} in {feedback_qualifier} records - as a single query");
            }

            FeedbackConditionResults("ad", "study_conditions", "icd_code");
        }
        catch (Exception e)
        {
            string eres = e.Message;
            _loggingHelper.LogError("In updating conditions using terms: " + eres);
        }
    }
    
    public void resolve_multiple_condition_entries()
    {
        // Consider only those with '//' in the term or code box. Thee have been created by the matching
        // process, if the original term matched two or more separate codes / terms, via the look up table.
        // Should normally all be in the most recently added / coded set only, but on a '(re)code all' could
        // be throughout the conditions table.
        
        int old_count = GetTableCount("study_conditions");
        
        string sql_string = $@"drop table if exists ad.temp_mult_conds;
        create table ad.temp_mult_conds as
        select c.* from ad.study_conditions c 
        where c.icd_code like '%//%'";
        ExecuteSQL(sql_string);
        
        int res = GetTableCount("temp_mult_conds");
        if (res > 0)
        {
            // If there are any such records, bring them out as a temp table and remove the records from
            // the original table. Split on the // to create separate lines (both the codes and the terms
            // are split at the same time). Add the split lines back in and delete the temporary table.
            
            _loggingHelper.LogLine($"{res} 'multiple condition' records found that needed splitting");
            sql_string = $@"delete from ad.study_conditions sc
                            using ad.temp_mult_conds mc
                            where sc.id = mc.id ";
            ExecuteSQL(sql_string);
            
            string base_sql = $@"insert into ad.study_conditions
                                (sd_sid, original_value, original_ct_type_id, original_ct_code,
		                        icd_code, icd_name, added_on, coded_on)
                            select c.sd_sid, c.original_value, 
                                c.original_ct_type_id, c.original_ct_code,
		                        trim(t.icd_code), trim(t.icd_name), 
		                        c.added_on, c.coded_on
                            from ad.temp_mult_conds c
                            cross join 
                                unnest(string_to_array(c.icd_code, '//'), string_to_array(c.icd_name, '//') ) 
                                as t(icd_code, icd_name) ";
            
            int min_id = GetMinId("temp_mult_conds");
            int max_id = GetMaxId("temp_mult_conds");
            int rec_batch = 50000;
            string action = "Inserting split records back into conditions table";
            try
            {
                if (max_id - min_id > rec_batch)
                {
                    for (int r = min_id; r <= max_id; r += rec_batch)
                    {
                        string batch_sql_string = base_sql + " where c.id >= " + r + " and c.id < " + (r + rec_batch);
                        int res1 = ExecuteSQL(batch_sql_string);
                        int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                        string feedback = $"{action} for {feedback_qualifier} records - {res1} in records {r} to {e}";
                        _loggingHelper.LogLine(feedback);
                    }
                }
                else
                {
                    int res2 = ExecuteSQL(base_sql);
                    _loggingHelper.LogLine($"{action} for {feedback_qualifier} records - {res2} done as a single query");
                }
            }
            catch (Exception e)
            {
                string eres = e.Message;
                _loggingHelper.LogError($"In {action}: " + eres);
            }
       
            // Delete the temporary table and log revised number of records.

            sql_string = $"drop table if exists ad.temp_mult_conds; ";
            ExecuteSQL(sql_string);
            int new_count = GetTableCount("study_conditions");
            if (new_count != old_count)
            {
                _loggingHelper.LogLine(
                    $"study_conditions table has {new_count} records, after splitting of compound conditions");
            }
        }
        else
        {
            _loggingHelper.LogLine("No 'multiple condition' records found that needed splitting");
        }
    }

    public int store_unmatched_condition_values(int source_id)
    {
        string sql_string = $"delete from context_ctx.to_match_conditions where source_id = {source_id}";
        ExecuteSQL(sql_string);
        sql_string = $@"insert into context_ctx.to_match_conditions (source_id, condition_value, number_of) 
                       select {source_id}, lower(original_value), count(lower(original_value)) 
                       from ad.study_conditions t 
                       where t.icd_code is null 
                       group by lower(original_value);";
        int res = ExecuteSQL(sql_string);
        _loggingHelper.LogLine($"Storing {res} condition codes not matched to ICD codes for review");
        return res;
    }
}
