using Dapper;
using Npgsql;

namespace MDR_Coder;

public class OrgHelper
{
    private readonly string _db_conn;
    private readonly ILoggingHelper _loggingHelper;   
    private readonly string study_scope_qualifier;
    private readonly string object_scope_qualifier;
    private readonly string feedback_qualifier;

    public OrgHelper(Source source, ILoggingHelper logger, int scope, bool recodeTestDataOnly)
    {
        _db_conn = source.db_conn ?? "";
        _loggingHelper = logger;
        study_scope_qualifier = scope == 1 ? " and c.coded_on is null " : "";
        object_scope_qualifier = study_scope_qualifier;
        feedback_qualifier = scope == 1 ? "unmatched" : "all";
        if (recodeTestDataOnly)
        {
            // test data 'trumps' the decisions above 
            study_scope_qualifier = " and c.sd_sid in (select sd_sid from mn.test_study_list) ";
            object_scope_qualifier = " and c.sd_oid in (select sd_oid from mn.test_object_list) ";
            feedback_qualifier = "test data";
        }
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
        string sql_string = @"drop table if exists ad.temp_org_names;
             create table ad.temp_org_names 
             as 
             select a.org_id, lower(a.name) as name from 
             context_ctx.org_names a
             where a.qualifier_id <> 10";

        Execute_SQL(sql_string);
    }

    // Code Study Organisations

    public void update_study_organisations()
    {
        int min_id = GetMinId("ad", "study_organisations");
        int max_id = GetMaxId("ad", "study_organisations");
        
        RemoveInitialThes("ad.study_organisations", "organisation_name", min_id, max_id, 200000);
        CodePharmaNames("study_organisations", "organisation_id", "organisation_name", object_scope_qualifier);
        
        string sql_string = $@"update ad.study_organisations c
                    set organisation_id = n.org_id
                    from ad.temp_org_names n
                    where c.organisation_name is not null
                    and lower(c.organisation_name) = n.name {study_scope_qualifier}";
        string action = $"Coding orgs for {feedback_qualifier} study organisations";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);   
        
        sql_string = $@"update ad.study_organisations c
                    set organisation_name = g.default_name,
                    organisation_ror_id = g.ror_id,
                    coded_on = CURRENT_TIMESTAMP    
                    from context_ctx.organisations g
                    where c.organisation_id = g.id {study_scope_qualifier}";
        action = $"Inserting default org data for {feedback_qualifier} study organisations";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults("ad", "study_organisations", "organisation_id", "organisation_ror_id");
    }

    public void CheckDupStudyOrganisations()
    {
         // Sometimes the same organisation can be added as sponsor and funder but under
         // different names. Their equality therefore only becomes apparent after coding.
         // This function identifies studies with contributors of 54 and 58 with the same
         // org code, and where they occur turns them into a single 112 coded record
         
         int min_id = GetMinId("ad", "studies");
         int max_id = GetMaxId("ad", "studies");

         string sql_string = @"Drop table if exists ad.temp_dup_org_ids;
                               Create table ad.temp_dup_org_ids
                               (sd_sid varchar, org_id int, 
                                id54 int default 0,
                                id58 int default 0 );";
         Execute_SQL(sql_string);

         string top_sql_string = @"Insert into ad.temp_dup_org_ids(sd_sid, org_id)
                    select g.sd_sid, g.organisation_id
                    from ad.study_organisations g 
                    inner join ad.studies c
                    on g.sd_sid = c.sd_sid 
                    where g.organisation_id is not null 
                    and g.contrib_type_id in (54,58)  ";

          string bottom_sql_string = @"group by g.sd_sid, g.organisation_id
                    having count(g.id) > 1 ;" ;
         
         string action = $"De-duplicating orgs (sponsors + funders) for {feedback_qualifier} study organisations";
         
         DeDupOrgs(min_id, max_id, 100000, top_sql_string, bottom_sql_string, action);   

         sql_string = @"Drop table if exists ad.temp_dup_org_ids;";
         Execute_SQL(sql_string);  
         
    }

    private void DeDupOrgs(int min_id, int max_id, int rec_batch, string top_sql_string,
               string bottom_sql_string, string action)
    {
        try
        {
            string sql_string;
            if (max_id - min_id > rec_batch)
            {
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    sql_string = "truncate table ad.temp_dup_org_ids;";
                    Execute_SQL(sql_string);

                    string batch_sql_string = top_sql_string
                                              + " and c.id >= " + r + " and c.id < " + (r + rec_batch)
                                              + bottom_sql_string;
                    int res1 = Execute_SQL(batch_sql_string);
                    int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                    string feedback =
                        $"{action} - {res1} records identified as potential duplicates, in ids {r} to {e}";
                    _loggingHelper.LogLine(feedback);
                    UseDuplicateOrgTableToAmendRecords();
                }
            }
            else
            {
                sql_string = top_sql_string + bottom_sql_string;
                int res = Execute_SQL(sql_string);
                _loggingHelper.LogLine(
                    $"{action} - {res} records identified as potential duplicates as a single query");
                UseDuplicateOrgTableToAmendRecords();
            }
        }
        catch (Exception e)
        {
            string eres = e.Message;
            _loggingHelper.LogError($"In {action}: " + eres);
        }
    }

    private void UseDuplicateOrgTableToAmendRecords()
    {
        // Obtain Ids of relevant study_organisations records

        string sql_string = @"update ad.temp_dup_org_ids d
        set id54 = g.id
        from ad.study_organisations g
        where d.sd_sid = g.sd_sid
        and d.org_id = g.organisation_id
        and g.contrib_type_id = 54;";
        Execute_SQL(sql_string);

        sql_string = @"update ad.temp_dup_org_ids d
        set id58 = g.id
        from ad.study_organisations g
        where d.sd_sid = g.sd_sid
        and d.org_id = g.organisation_id
        and g.contrib_type_id = 58;";
        Execute_SQL(sql_string);

        // restrict the records to those with both type 54 and 58 contribs
        // (54 = sponsor, 58 = funder)

        sql_string = @"delete from ad.temp_dup_org_ids d
        where id54 = 0 or id58 = 0;";
        Execute_SQL(sql_string);

        // update the study_organisations records to indicate joint role

        sql_string = @"update ad.study_organisations g
        set contrib_type_id = 112
        from ad.temp_dup_org_ids d
        where g.id = d.id54;";
        Execute_SQL(sql_string);

        // delete the now superfluous study_organisations records

        sql_string = @"delete from ad.study_organisations g
        using ad.temp_dup_org_ids d
        where g.id = d.id58;";
        Execute_SQL(sql_string);
    }
 

    // Code Study Identifiers

    public void update_study_identifiers()
    {
        int min_id = GetMinId("ad", "study_identifiers");
        int max_id = GetMaxId("ad", "study_identifiers");
        
        RemoveInitialThes("ad.study_identifiers", "source", min_id, max_id, 200000);
        CodePharmaNames("study_identifiers", "source_id", "source", object_scope_qualifier);
        
        string sql_string = $@"update ad.study_identifiers c
                    set source_id = n.org_id   
                    from ad.temp_org_names n
                    where lower(c.source) = n.name {study_scope_qualifier}";
        string action = $"Coding sources for {feedback_qualifier} study identifiers";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
    
        sql_string = $@"update ad.study_identifiers c
                    set source = g.default_name,
                    source_ror_id = g.ror_id,
                    coded_on = CURRENT_TIMESTAMP 
                    from context_ctx.organisations g
                    where c.source_id = g.id {study_scope_qualifier}";
        action = $"Inserting default org data for {feedback_qualifier} study identifiers";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults("ad", "study_identifiers", "source_id", "source_ror_id");

        // seems to only apply to some CTG records (need to have done study orgs first)
       
        sql_string = $@"update ad.study_identifiers c
               set source_id = sc.organisation_id,
               source = sc.organisation_name,
               source_ror_id = sc.organisation_ror_id,
               coded_on = CURRENT_TIMESTAMP    
               from ad.study_organisations sc
               where c.sd_sid = sc.sd_sid
               and (c.source ilike 'sponsor' 
               or c.source ilike 'company internal')
               and sc.contrib_type_id = 54 ";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, "Updating org data for sponsor study identifiers");
    }
    

    // Code Study People

    public void update_study_people()
    {
        int min_id = GetMinId("ad", "study_people");
        int max_id = GetMaxId("ad", "study_people");
        
        RemoveInitialThes("ad.study_people", "organisation_name",min_id, max_id, 200000);
        CodePharmaNames("study_people", "organisation_id", "organisation_name", object_scope_qualifier);
        
        string sql_string = $@"update ad.study_people c
                    set organisation_id = n.org_id
                    from ad.temp_org_names n
                    where c.organisation_id is null
                    and c.organisation_name is not null
                    and lower(c.organisation_name) = n.name {study_scope_qualifier}";
        string action = $"Coding orgs for {feedback_qualifier} study people";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);

        sql_string = $@"update ad.study_people c
                    set organisation_name = g.default_name,
                    organisation_ror_id = g.ror_id,
                    coded_on = CURRENT_TIMESTAMP    
                    from context_ctx.organisations g
                    where c.organisation_id = g.id {study_scope_qualifier}";
        action = $"Inserting default org data for {feedback_qualifier} study people";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        
        FeedbackResults("ad", "study_people", "organisation_id", "organisation_ror_id");
    }
    

    // Code Object Identifiers

    public void update_object_identifiers()
    {
        int min_id = GetMinId("ad", "object_identifiers");
        int max_id = GetMaxId("ad", "object_identifiers");
        
        RemoveInitialThes("ad.object_identifiers", "source", min_id, max_id, 200000);
        CodePharmaNames("object_identifiers", "source_id", "source", object_scope_qualifier);
        
        string sql_string = $@"update ad.object_identifiers c
                    set source_id = n.org_id   
                    from ad.temp_org_names n
                    where source_id is null 
                    and lower(c.source) = n.name {object_scope_qualifier}";
        string action = $"Coding sources for {feedback_qualifier} object identifiers";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
    
        sql_string = $@"update ad.object_identifiers c
                    set source = g.default_name,
                    source_ror_id = g.ror_id,
                    coded_on = CURRENT_TIMESTAMP 
                    from context_ctx.organisations g
                    where c.source_id = g.id {object_scope_qualifier}";
        action = $"Inserting default org data for {feedback_qualifier} object identifiers";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults("ad", "object_identifiers", "source_id", "source_ror_id");
    }


    // Code Object Organisations

    public void update_object_organisations()
    {
        int min_id = GetMinId("ad", "object_organisations");
        int max_id = GetMaxId("ad", "object_organisations");
        
        RemoveInitialThes("ad.object_organisations", "organisation_name", min_id, max_id, 200000);
        CodePharmaNames("object_organisations", "organisation_id", "organisation_name", object_scope_qualifier);
        
        string sql_string = $@"update ad.object_organisations c
                    set organisation_id = n.org_id
                    from ad.temp_org_names n
                    where c.organisation_id is null
                    and c.organisation_name is not null
                    and lower(c.organisation_name) = n.name {object_scope_qualifier}";
        string action = $"Coding orgs for {feedback_qualifier} object organisations";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        
        sql_string = $@"update ad.object_organisations c
                    set organisation_name = g.default_name,
                    organisation_ror_id = g.ror_id,
                    coded_on = CURRENT_TIMESTAMP    
                    from context_ctx.organisations g
                    where c.organisation_id = g.id {object_scope_qualifier}";
        action = $"Inserting default org data for {feedback_qualifier} object organisations";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults("ad", "object_organisations", "organisation_id", "organisation_ror_id");
    }

    
    // Code Object People

    public void update_object_people()
    {
        int min_id = GetMinId("ad", "object_people");
        int max_id = GetMaxId("ad", "object_people");
        
        RemoveInitialThes("ad.object_people", "organisation_name", min_id, max_id, 200000);
        CodePharmaNames("object_people", "organisation_id", "organisation_name", object_scope_qualifier);
        
        string sql_string = $@"update ad.object_people c
                    set organisation_id = n.org_id
                    from ad.temp_org_names n
                    where c.organisation_id is null
                    and c.organisation_name is not null
                    and lower(c.organisation_name) = n.name {object_scope_qualifier}";
        string action = $"Coding orgs for {feedback_qualifier} object people";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        
        sql_string = $@"update ad.object_people c
                    set organisation_name = g.default_name,
                    organisation_ror_id = g.ror_id,
                    coded_on = CURRENT_TIMESTAMP    
                    from context_ctx.organisations g
                    where c.organisation_id = g.id {object_scope_qualifier}";
        action = $"Inserting default org data for {feedback_qualifier} object people";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults("ad", "object_people", "organisation_id", "organisation_ror_id");
    }
    

    // Code Data Object Organisations

    public void update_data_objects()
    {
        int min_id = GetMinId("ad", "data_objects");
        int max_id = GetMaxId("ad", "data_objects");
        
        RemoveInitialThes("ad.data_objects", "managing_org", min_id, max_id, 200000);
        CodePharmaNames("data_objects", "managing_org_id", "managing_org", object_scope_qualifier);
        
        string sql_string = $@"update ad.data_objects c
                    set managing_org_id = n.org_id     
                    from ad.temp_org_names n
                    where lower(c.managing_org) = n.name
                    and c.managing_org_id is null {object_scope_qualifier}";
        string action = $"Coding managing orgs for {feedback_qualifier} data objects";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        
        sql_string = $@"update ad.data_objects c
                    set managing_org = g.default_name,
                    managing_org_ror_id = g.ror_id,
                    coded_on = CURRENT_TIMESTAMP     
                    from context_ctx.organisations g
                    where c.managing_org_id = g.id {object_scope_qualifier}";
        action = $"Inserting default org data for {feedback_qualifier} data objects";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults("ad", "data_objects", "managing_org_id", "managing_org_ror_id");
    }
    
    
    // Code Object Instances

    public void update_object_instances()
    {
        int min_id = GetMinId("ad", "object_instances");
        int max_id = GetMaxId("ad", "object_instances");
        
        RemoveInitialThes("ad.object_instances", "system", min_id, max_id, 200000);
        CodePharmaNames("object_instances", "system_id", "system", object_scope_qualifier);
        
        string sql_string = $@"update ad.object_instances c
                    set system_id = n.org_id
                    from ad.temp_org_names n
                    where c.system_id is null
                    and c.system is not null
                    and lower(c.system) = n.name {object_scope_qualifier}";
        string action = $"Coding orgs for {feedback_qualifier} object instances";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        
        sql_string = $@"update ad.object_instances c
                    set system = g.default_name,
                    coded_on = CURRENT_TIMESTAMP
                    from context_ctx.organisations g
                    where c.system_id = g.id {object_scope_qualifier}";
        action = $"Inserting default org data for {feedback_qualifier} object instances";
        Execute_OrgSQL(min_id, max_id, 200000, sql_string, action);
        FeedbackResults("ad", "object_instances", "system_id", "");
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
        string sql_string = $@"drop table ad.temp_org_names;";

        Execute_SQL(sql_string);
    }

    private void CodePharmaNames(string table_name, string field_to_update, string field_to_check, string scope_qualifier)
    {
        int res = 0;
        res += CodePharma(table_name, field_to_update, "100189", $" where {field_to_check} ilike 'novartis%' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100200", $" where {field_to_check} ilike 'novo nordisk%' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "109369", $" where {field_to_check} ilike '%viatris%' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100164", $" where ({field_to_check} ilike 'pfizer%' and {field_to_check} not ilike '%viatris%') {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100227", $" where {field_to_check} ilike 'takeda%' {scope_qualifier} ");
        res += CodePharma(table_name, field_to_update, "100163", $" where ({field_to_check} ilike 'gsk%' or {field_to_check} ilike 'glaxo%') {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100166", $" where {field_to_check} ilike 'astrazen%' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100385", $" where ({field_to_check} ilike 'sanofi%' and {field_to_check} ilike '%pasteur%') {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100180", $" where ({field_to_check} ilike 'sanofi%' and {field_to_check} not ilike '%pasteur%') {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "107838", $" where {field_to_check} ilike 'regeneron%' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100173", $" where {field_to_check} ilike 'boehringer%' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "109370", $" where {field_to_check} ilike 'moderna%' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100179", $" where {field_to_check} ilike 'bayer %' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100232", $" where {field_to_check} ilike 'gilead %' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100207", $" where {field_to_check} ilike 'amgen %' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100176", $" where {field_to_check} ilike 'eli lilly%' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100165", $" where ({field_to_check} ilike '%merck%' and {field_to_check} ilike '%sharp%') {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100165", $" where {field_to_check} ilike 'msd %' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100341", $" where ({field_to_check} ilike 'merck%' and {field_to_check} not ilike '%sharp%') {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100175", $" where ({field_to_check} ilike 'bristol%' and {field_to_check} ilike '%myers%')  {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100175", $" where {field_to_check} ilike 'bms' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100172", $" where ({field_to_check} ilike '%hoffmann%' and {field_to_check} ilike '%roche%') {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100172", $" where {field_to_check} ilike '%genentech%' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100290", $" where {field_to_check} ilike 'johnson & johnson%' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100107", $" where {field_to_check} ilike '%janssen%' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100288", $" where {field_to_check} ilike 'abbvie%' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "105070", $" where {field_to_check} ilike 'biontech%' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100254", $" where {field_to_check} ilike 'astellas%' {scope_qualifier}");
        res += CodePharma(table_name, field_to_update, "100321", $" where {field_to_check} ilike 'biogen %' {scope_qualifier}");
        
        _loggingHelper.LogLine($"{res} records coded against common pharma names in {table_name}");
    }
    

    private int CodePharma(string table_name, string field_to_update, string id_value, string where_clause)
    {
        string sql_string = $@"update ad.{table_name} c
        set {field_to_update} = {id_value} {where_clause}";
        return Execute_SQL(sql_string);
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
        from ad.study_identifiers 
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
        from ad.study_organisations 
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
        from ad.study_people 
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
        from ad.object_identifiers 
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
        from ad.object_organisations 
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
        from ad.object_people 
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
        from ad.data_objects 
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
        from ad.object_instances 
        where system_id is null 
        group by system; ";

        int res = Execute_SQL(sql_string); 
        _loggingHelper.LogLine($"Stored {res} unmatched object instance organisation names, for review");
        return res;
    }
}

