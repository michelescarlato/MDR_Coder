using Dapper;
using Npgsql;

namespace MDR_Coder;

public class OrgHelper
{
    private readonly string _db_conn;
    private readonly string _schema;
    private readonly ILoggingHelper _loggingHelper;     

    public OrgHelper(Source source, ILoggingHelper logger)
    {
        _db_conn = source.db_conn ?? "";
        _schema = "ad"; 
        _loggingHelper = logger;
    }

    public int Execute_SQL(string sql_string)
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

    
    private void FeedbackResults(string schema, string table_name, string id_field, string ror_field)
    {
        int table_count = GetTableCount(schema, table_name);
        int coded_count = GetFieldCount(schema, table_name, id_field);
        _loggingHelper.LogLine($"{coded_count} records, from {table_count}, " +
                               $"{(double) 100 * coded_count / table_count:N1} %, " +  
                               $"have MDR coded organisations in {schema}.{table_name}");
        if (ror_field != "")
        {
            coded_count = GetFieldCount(schema, table_name, ror_field);
            _loggingHelper.LogLine($"{coded_count} records, from {table_count}, " +
                                   $"{(double) 100 * coded_count / table_count:N1} %, " + 
                                   $"have ROR coded organisations in {schema}.{table_name}");
        }
    }
    
    
    // Set up relevant names for comparison
    public void establish_temp_tables()
    {
        string sql_string = @"drop table if exists " + _schema + @".temp_org_names;
             create table " + _schema + @".temp_org_names 
             as 
             select a.org_id, lower(a.name) as name from 
             context_ctx.org_names a
             where a.qualifier_id <> 10";

        Execute_SQL(sql_string);
    }

    // Code Study Organisations

    public void update_study_organisations(bool code_all)
    {
        int min_id = GetMinId(_schema, "study_organisations");
        int max_id = GetMaxId(_schema, "study_organisations");
        string qualifier = code_all ? "all" : "unmatched"; 
        
        RemoveInitialThes(_schema + ".study_organisations", "organisation_name", min_id, max_id, 200000);
        
        string sql_string = $@"update {_schema}.study_organisations c
        set organisation_id = n.org_id
        from {_schema}.temp_org_names n
        where c.organisation_name is not null
        and lower(c.organisation_name) = n.name ";
        sql_string += code_all ? "" : " and coded_on is null ";
        
        string action = $"Coding orgs for {qualifier} study organisations";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);   
        
        sql_string = $@"update {_schema}.study_organisations c
        set organisation_name = g.default_name,
        organisation_ror_id = g.ror_id,
        coded_on = CURRENT_TIMESTAMP    
        from context_ctx.organisations g
        where c.organisation_id = g.id ";
        
        action = $"Inserting default org data for {qualifier} study organisations";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults(_schema, "study_organisations", "organisation_id", "organisation_ror_id");
    }
    
    // Code Study Identifiers

    public void update_study_identifiers(bool code_all)
    {
        int min_id = GetMinId(_schema, "study_identifiers");
        int max_id = GetMaxId(_schema, "study_identifiers");
        string qualifier = code_all ? "all" : "unmatched"; 
        
        RemoveInitialThes(_schema + ".study_identifiers", "source", min_id, max_id, 200000);
        
        string sql_string = $@"update {_schema}.study_identifiers c
        set source_id = n.org_id   
        from {_schema}.temp_org_names n
        where lower(c.source) = n.name ";
        sql_string += code_all ? "" : " and coded_on is null ";
        
        string action = $"Coding sources for {qualifier} study identifiers";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
    
        sql_string = $@"update {_schema}.study_identifiers c
        set source = g.default_name,
        source_ror_id = g.ror_id,
        coded_on = CURRENT_TIMESTAMP 
        from context_ctx.organisations g
        where c.source_id = g.id ";
        
        action = $"Inserting default org data for {qualifier} study identifiers";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults(_schema, "study_identifiers", "source_id", "source_ror_id");

        // seems to only apply to some CTG records (need to have done study orgs first)
       
        sql_string = $@"update {_schema}.study_identifiers c
               set source_id = sc.organisation_id,
               source = sc.organisation_name,
               source_ror_id = sc.organisation_ror_id,
               coded_on = CURRENT_TIMESTAMP    
               from {_schema}.study_organisations sc
               where c.sd_sid = sc.sd_sid
               and (c.source ilike 'sponsor' 
               or c.source ilike 'company internal')
               and sc.contrib_type_id = 54 ";

        Execute_OrgSQL(min_id, max_id, 200000, sql_string, "Updating org data for sponsor study identifiers");
    }
    

    // Code Study People

    public void update_study_people(bool code_all)
    {
        int min_id = GetMinId(_schema, "study_people");
        int max_id = GetMaxId(_schema, "study_people");
        string qualifier = code_all ? "all" : "unmatched"; 
        
        RemoveInitialThes(_schema + ".study_people", "organisation_name",min_id, max_id, 200000);
        
        string sql_string = $@"update {_schema}.study_people c
        set organisation_id = n.org_id
        from {_schema}.temp_org_names n
        where c.organisation_id is null
        and c.organisation_name is not null
        and lower(c.organisation_name) = n.name ";
        sql_string += code_all ? "" : " and coded_on is null ";
        
        string action = $"Coding orgs for {qualifier} study people";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);

        sql_string = $@"update {_schema}.study_people c
        set organisation_name = g.default_name,
        organisation_ror_id = g.ror_id,
        coded_on = CURRENT_TIMESTAMP    
        from context_ctx.organisations g
        where c.organisation_id = g.id ";
        
        action = $"Inserting default org data for {qualifier} study people";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults(_schema, "study_people", "organisation_id", "organisation_ror_id");
    }
    

    // Code Object Identifiers

    public void update_object_identifiers(bool code_all)
    {
        int min_id = GetMinId(_schema, "object_identifiers");
        int max_id = GetMaxId(_schema, "object_identifiers");
        string qualifier = code_all ? "all" : "unmatched"; 
        
        RemoveInitialThes(_schema + ".object_identifiers", "source", min_id, max_id, 200000);
        
        string sql_string = $@"update {_schema}.object_identifiers c
        set source_id = n.org_id   
        from {_schema}.temp_org_names n
        where source_id is null 
        and lower(c.source) = n.name ";
        sql_string += code_all ? "" : " and coded_on is null ";
        
        string action = $"Coding sources for {qualifier} object identifiers";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
    
        sql_string = $@"update {_schema}.object_identifiers c
        set source = g.default_name,
        source_ror_id = g.ror_id,
        coded_on = CURRENT_TIMESTAMP 
        from context_ctx.organisations g
        where c.source_id = g.id ";
        
        action = $"Inserting default org data for {qualifier} object identifiers";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults(_schema, "object_identifiers", "source_id", "source_ror_id");
    }


    // Code Object Organisations

    public void update_object_organisations(bool code_all)
    {
        int min_id = GetMinId(_schema, "object_organisations");
        int max_id = GetMaxId(_schema, "object_organisations");
        string qualifier = code_all ? "all" : "unmatched"; 
        
        RemoveInitialThes(_schema + ".object_organisations", "organisation_name", min_id, max_id, 200000);
        
        string sql_string = $@"update {_schema}.object_organisations c
        set organisation_id = n.org_id
        from {_schema}.temp_org_names n
        where c.organisation_id is null
        and c.organisation_name is not null
        and lower(c.organisation_name) = n.name ";
        sql_string += code_all ? "" : " and coded_on is null ";
        
        string action = $"Coding orgs for {qualifier} object organisations";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        
        sql_string = $@"update {_schema}.object_organisations c
        set organisation_name = g.default_name,
        organisation_ror_id = g.ror_id,
        coded_on = CURRENT_TIMESTAMP    
        from context_ctx.organisations g
        where c.organisation_id = g.id ";
        
        action = $"Inserting default org data for {qualifier} object organisations";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults(_schema, "object_organisations", "organisation_id", "organisation_ror_id");
    }

    
    // Code Object People

    public void update_object_people(bool code_all)
    {
        int min_id = GetMinId(_schema, "object_people");
        int max_id = GetMaxId(_schema, "object_people");
        string qualifier = code_all ? "all" : "unmatched"; 
        
        RemoveInitialThes(_schema + ".object_people", "organisation_name", min_id, max_id, 200000);
        
        string sql_string = $@"update {_schema}.object_people c
        set organisation_id = n.org_id
        from {_schema}.temp_org_names n
        where c.organisation_id is null
        and c.organisation_name is not null
        and lower(c.organisation_name) = n.name ";
        sql_string += code_all ? "" : " and coded_on is null ";
        
        string action = $"Coding orgs for {qualifier} object people";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        
        sql_string = $@"update {_schema}.object_people c
        set organisation_name = g.default_name,
        organisation_ror_id = g.ror_id,
        coded_on = CURRENT_TIMESTAMP    
        from context_ctx.organisations g
        where c.organisation_id = g.id ";
        
        action = $"Inserting default org data for {qualifier} object people";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults(_schema, "object_people", "organisation_id", "organisation_ror_id");
    }
    

    // Code Data Object Organisations

    public void update_data_objects(bool code_all)
    {
        int min_id = GetMinId(_schema, "data_objects");
        int max_id = GetMaxId(_schema, "data_objects");
        string qualifier = code_all ? "all" : "unmatched"; 
        
        RemoveInitialThes(_schema + ".data_objects", "managing_org", min_id, max_id, 200000);
        
        string sql_string = $@"update {_schema}.data_objects c
        set managing_org_id = n.org_id     
        from {_schema}.temp_org_names n
        where lower(c.managing_org) = n.name
        and c.managing_org_id is null ";
        sql_string += code_all ? "" : " and coded_on is null ";
        
        string action = $"Coding managing orgs for {qualifier} data objects";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        
        sql_string = $@"update {_schema}.data_objects c
        set managing_org = g.default_name,
        managing_org_ror_id = g.ror_id,
        coded_on = CURRENT_TIMESTAMP     
        from context_ctx.organisations g
        where c.managing_org_id = g.id ";
        
        action = $"Inserting default org data for {qualifier} data objects";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults(_schema, "data_objects", "managing_org_id", "managing_org_ror_id");
    }
    
    
    // Code Object Instances

    public void update_object_instances(bool code_all)
    {
        int min_id = GetMinId(_schema, "object_instances");
        int max_id = GetMaxId(_schema, "object_instances");
        string qualifier = code_all ? "all" : "unmatched"; 
        
        RemoveInitialThes(_schema + ".object_instances", "system", min_id, max_id, 200000);
        
        string sql_string = $@"update {_schema}.object_instances c
        set system_id = n.org_id
        from {_schema}.temp_org_names n
        where c.system_id is null
        and c.system is not null
        and lower(c.system) = n.name ";
        sql_string += code_all ? "" : " and coded_on is null ";
        
        string action = $"Coding orgs for {qualifier} object instances";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        
        sql_string = $@"update {_schema}.object_instances c
        set system = g.default_name,
        coded_on = CURRENT_TIMESTAMP
        from context_ctx.organisations g
        where c.system_id = g.id ";
        
        action = $"Inserting default org data for {qualifier} object instances";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults(_schema, "object_instances", "system_id", "");
    }

    private void Execute_OrgSQL(int min_id, int max_id, int rec_batch, string base_sql, string action)
    {
        try
        {
            if (max_id - min_id > rec_batch)
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = base_sql + " and c.id >= " + r + " and c.id < " + (r + rec_batch);
                    int res1 = Execute_SQL(batch_sql_string);
                    int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                    string feedback = $"{action} - {res1} in records {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                int res = Execute_SQL(base_sql);
                _loggingHelper.LogLine($"{action} - {res} done as a single query");
            }
        }
        catch (Exception e)
        {
            string eres = e.Message;
            _loggingHelper.LogError($"In {action}: " + eres);
        }
    }
    
    
    public void delete_temp_tables()
    {
        string sql_string = $@"drop table {_schema}.temp_org_names;";

        Execute_SQL(sql_string);
    }
    

    private void RemoveInitialThes(string table_name, string field_name, int min_id, int max_id,  int rec_batch)
    {
        string action = $"Removing initial 'The's from org names in {table_name}";
        
        string base_sql = $@"update {table_name} c
        set {field_name} = trim(substring({field_name}, 4)) 
        where {field_name} ilike 'The %'
        and cardinality(string_to_array({field_name} , ' ')) > 2";
        
        try
        {
            if (max_id - min_id > rec_batch)
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = base_sql + " and c.id >= " + r + " and c.id < " + (r + rec_batch);
                    int res1 = Execute_SQL(batch_sql_string);
                    int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                    string feedback = $"{action} - {res1} records in ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                }
            }
            else
            {
                int res = Execute_SQL(base_sql);
                _loggingHelper.LogLine($"{action} - {res} records done as a single query");
            }
        }
        catch (Exception e)
        {
            string eres = e.Message;
            _loggingHelper.LogError($"In {action}: " + eres);
        }
    }
    

    // Store unmatched names

    public int store_unmatched_study_identifiers_org_names(int source_id)
    {
        string sql_string = $@"delete from context_ctx.to_match_orgs 
                               where source_id = {source_id}  
                               and source_table = 'study_identifiers';";
        Execute_SQL(sql_string); 
        sql_string = $@"insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
        select {source_id}, 'study_identifiers', source, count(source) 
        from {_schema}.study_identifiers 
        where source_id is null 
        group by source; ";

        int res = Execute_SQL(sql_string);
        _loggingHelper.LogLine($"Stored {res} unmatched study identifier organisation names, for review");
        return res;
    }

    public int store_unmatched_study_organisation_names(int source_id)
    {
        string sql_string = $@"delete from context_ctx.to_match_orgs 
                               where source_id = {source_id}  
                               and source_table = 'study_organisations';";
        Execute_SQL(sql_string); 
        sql_string = $@"insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
        select {source_id}, 'study_organisations', organisation_name, count(organisation_name) 
        from {_schema}.study_organisations 
        where organisation_id is null 
        group by organisation_name;";

        int res = Execute_SQL(sql_string);
        _loggingHelper.LogLine($"Stored {res} unmatched study organisation names, for review");
        return res;
    }

    public int store_unmatched_study_people_org_names(int source_id)
    {
        string sql_string = $@"delete from context_ctx.to_match_orgs 
                               where source_id = {source_id}  
                               and source_table = 'study_people';";
        Execute_SQL(sql_string); 
        sql_string = $@"insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
        select {source_id}, 'study_people', organisation_name, count(organisation_name) 
        from {_schema}.study_people 
        where organisation_id is null 
        group by organisation_name;";

        int res = Execute_SQL(sql_string);
        _loggingHelper.LogLine($"Stored {res} unmatched study people organisation names, for review");
        return res;
    }

    public int store_unmatched_object_identifiers_org_names(int source_id)
    {
        string sql_string = $@"delete from context_ctx.to_match_orgs 
                               where source_id = {source_id}  
                               and source_table = 'object_identifiers';";
        Execute_SQL(sql_string); 
        sql_string = $@"insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
        select {source_id}, 'object_identifiers', source, count(source) 
        from {_schema}.object_identifiers 
        where source_id is null 
        group by source; ";

        int res = Execute_SQL(sql_string);
        _loggingHelper.LogLine($"Stored {res} unmatched study identifier organisation names, for review");
        return res;
    }


    public int store_unmatched_object_organisation_org_names(int source_id)
    {
        string sql_string = $@"delete from context_ctx.to_match_orgs 
                               where source_id = {source_id} 
                               and source_table = 'object_organisations';";
        Execute_SQL(sql_string); 
        sql_string = $@"insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
        select {source_id}, 'object_organisations', organisation_name, count(organisation_name) 
        from {_schema}.object_organisations 
        where organisation_id is null 
        group by organisation_name;";

        int res = Execute_SQL(sql_string);
        _loggingHelper.LogLine($"Stored {res} unmatched object organisation names, for review");
        return res;
    }
    
    
    public int store_unmatched_object_people_org_names(int source_id)
    {
        string sql_string = $@"delete from context_ctx.to_match_orgs 
                               where source_id = {source_id}  
                               and source_table = 'object_people';";
        Execute_SQL(sql_string); 
        sql_string = $@"insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
        select {source_id}, 'object_people', organisation_name, count(organisation_name) 
        from {_schema}.object_people 
        where organisation_id is null 
        group by organisation_name;";

        int res = Execute_SQL(sql_string);
        _loggingHelper.LogLine($"Stored {res} unmatched object people organisation names, for review");
        return res;
    }
    

    public int store_unmatched_data_object_org_names(int source_id)
    {
        string sql_string = $@"delete from context_ctx.to_match_orgs 
                               where source_id = {source_id} 
                               and source_table = 'data_objects';";
        Execute_SQL(sql_string); 
        sql_string = $@"insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
        select {source_id}, 'data_objects', managing_org, count(managing_org) 
        from {_schema}.data_objects 
        where managing_org_id is null 
        group by managing_org; ";

        int res = Execute_SQL(sql_string); 
        _loggingHelper.LogLine($"Stored {res} unmatched object managing organisation names, for review");
        return res;
    }
    
    
    public int store_unmatched_object_instance_org_names(int source_id)
    {
        string sql_string = $@"delete from context_ctx.to_match_orgs 
                               where source_id = {source_id} 
                               and source_table = 'object_instances';";
        Execute_SQL(sql_string); 
        sql_string = $@"insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
        select {source_id}, 'object_instances', system, count(system) 
        from {_schema}.object_instances 
        where system_id is null 
        group by system; ";

        int res = Execute_SQL(sql_string); 
        _loggingHelper.LogLine($"Stored {res} unmatched object instance organisation names, for review");
        return res;
    }
}

