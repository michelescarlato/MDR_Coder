using Dapper;
using Npgsql;

namespace MDR_Coder
{
    public class OrgHelper
    {
        private readonly string _db_conn;
        private readonly string _schema;
        private readonly ILoggingHelper _loggingHelper;     

        public OrgHelper(Source source, ILoggingHelper logger)
        {
            _db_conn = source.db_conn ?? "";
            _schema = source.source_type == "test" ? "expected" : "ad"; 
            _loggingHelper = logger;
        }

        public int Execute_SQL(string sql_string)
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

        
        private void FeedbackResults(string schema, string table_name, string id_field, string ror_field)
        {
            int table_count = GetTableCount(schema, table_name);
            int org_id_count = GetFieldCount(schema, table_name, id_field);
            _loggingHelper.LogLine($"{org_id_count} records, from {table_count}, " + 
                                   $"have MDR coded organisations in {schema}.{table_name}");
            if (ror_field != "")
            {
                int ror_id_count = GetFieldCount(schema, table_name, ror_field);
                _loggingHelper.LogLine($"{ror_id_count} records, from {table_count}, " +
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
            
            sql_string = @"drop table if exists " + _schema + @".temp_country_names;
                 create table " + _schema + @".temp_country_names 
                 as 
                 select a.geoname_id, country_name, lower(a.alt_name) as name from 
                 context_ctx.country_names a";

            Execute_SQL(sql_string);
            
            sql_string = @"drop table if exists " + _schema + @".temp_city_names;
                 create table " + _schema + @".temp_city_names 
                 as 
                 select a.geoname_id, city_name, lower(a.alt_name) as name, country_id, country_name from 
                 context_ctx.city_names a";

            Execute_SQL(sql_string);
        }


        // Study identifier Organisations

        public void update_study_identifiers(bool code_all)
        {
            int rec_count = GetTableCount(_schema, "study_identifiers");
            RemoveInitialThes(_schema + ".study_identifiers", "source", rec_count, 200000);
            
            string sql_string = @"update " + _schema + @".study_identifiers c
            set source_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP    
            from " + _schema + @".temp_org_names n
            where source_id is null 
            and lower(c.source) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";

            Execute_OrgSQL(rec_count, 200000, sql_string, "Coding sources for study identifiers");
        
            sql_string = @"update " + _schema + @".study_identifiers c
            set source = g.default_name,
            source_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.source_id = g.id ";
            
            Execute_OrgSQL(rec_count, 200000, sql_string, "Inserting default org data for study identifiers");
            FeedbackResults(_schema, "study_identifiers", "source_id", "source_ror_id");
        }


        // Study contributor Organisations

        public void update_study_organisations(bool code_all)
        {
            int rec_count = GetTableCount(_schema, "study_organisations");
            RemoveInitialThes(_schema + ".study_organisations", "organisation_name", rec_count, 200000);
            
            string sql_string = @"update " + _schema + @".study_organisations c
            set organisation_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP    
            from " + _schema + @".temp_org_names n
            where c.organisation_id is null
            and c.organisation_name is not null
            and lower(c.organisation_name) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            Execute_OrgSQL(rec_count, 200000, sql_string, "Coding orgs for study organisations");   
            
            sql_string = @"update " + _schema + @".study_organisations c
            set organisation_name = g.default_name,
            organisation_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.organisation_id = g.id ";

            Execute_OrgSQL(rec_count, 200000, sql_string, "Inserting default org data for study organisations");
            FeedbackResults(_schema, "study_organisations", "organisation_id", "organisation_ror_id");
        }


        public void update_missing_sponsor_ids()
        {
            // seems to only apply to some CTG records
            
            int rec_count = GetTableCount(_schema, "study_identifiers");
            
            string sql_string = @"update " + _schema + @".study_identifiers c
                   set source_id = sc.organisation_id,
                   source = sc.organisation_name,
                   source_ror_id = sc.organisation_ror_id,
                   coded_on = CURRENT_TIMESTAMP    
                   from " + _schema + @".study_organisations sc
                   where c.sd_sid = sc.sd_sid
                   and (c.source ilike 'sponsor' 
                   or c.source ilike 'company internal')
                   and sc.contrib_type_id = 54 ";

            Execute_OrgSQL(rec_count, 200000, sql_string, "Updating org data for study identifiers");
        }
        

        // Study contributor people

        public void update_study_people(bool code_all)
        {
            int rec_count = GetTableCount(_schema, "study_people");
            RemoveInitialThes(_schema + ".study_people", "organisation_name", rec_count, 200000);
            
            string sql_string = @"update " + _schema + @".study_people c
            set organisation_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP    
            from " + _schema + @".temp_org_names n
            where c.organisation_id is null
            and c.organisation_name is not null
            and lower(c.organisation_name) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            Execute_OrgSQL(rec_count, 200000, sql_string, "Coding orgs for study people");

            sql_string = @"update " + _schema + @".study_people c
            set organisation_name = g.default_name,
            organisation_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.organisation_id = g.id ";
            
            Execute_OrgSQL(rec_count, 200000, sql_string, "Inserting default org data for study people");
            FeedbackResults(_schema, "study_people", "organisation_id", "organisation_ror_id");
        }
        

        // Object identifier Organisations

        public void update_object_identifiers(bool code_all)
        {
            int rec_count = GetTableCount(_schema, "object_identifiers");
            RemoveInitialThes(_schema + ".object_identifiers", "source", rec_count, 200000);
            
            string sql_string = @"update " + _schema + @".object_identifiers c
            set source_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP    
            from " + _schema + @".temp_org_names n
            where source_id is null 
            and lower(c.source) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            Execute_OrgSQL(rec_count, 200000, sql_string, "Coding sources for object identifiers");
        
            sql_string = @"update " + _schema + @".object_identifiers c
            set source = g.default_name,
            source_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.source_id = g.id ";
 
            Execute_OrgSQL(rec_count, 200000, sql_string, "Inserting default org data for object identifiers");
            FeedbackResults(_schema, "object_identifiers", "source_id", "source_ror_id");
        }


        // Object contributor Organisations

        public void update_object_organisations(bool code_all)
        {
            int rec_count = GetTableCount(_schema, "object_organisations");
            RemoveInitialThes(_schema + ".object_organisations", "organisation_name", rec_count, 200000);
            
            string sql_string = @"update " + _schema + @".object_organisations c
            set organisation_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP    
            from " + _schema + @".temp_org_names n
            where c.organisation_id is null
            and c.organisation_name is not null
            and lower(c.organisation_name) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";

            Execute_OrgSQL(rec_count, 200000, sql_string, "Coding orgs for object organisations");
            
            sql_string = @"update " + _schema + @".object_organisations c
            set organisation_name = g.default_name,
            organisation_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.organisation_id = g.id ";

            Execute_OrgSQL(rec_count, 200000, sql_string, "Inserting default org data for object organisations");
            FeedbackResults(_schema, "object_organisations", "organisation_id", "organisation_ror_id");
        }

        
        // Object People

        public void update_object_people(bool code_all)
        {
            int rec_count = GetTableCount(_schema, "object_people");
            RemoveInitialThes(_schema + ".object_people", "organisation_name", rec_count, 200000);
            
            string sql_string = @"update " + _schema + @".object_people c
            set organisation_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP    
            from " + _schema + @".temp_org_names n
            where c.organisation_id is null
            and c.organisation_name is not null
            and lower(c.organisation_name) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";

            Execute_OrgSQL(rec_count, 200000, sql_string, "Coding orgs for object people");
            
            sql_string = @"update " + _schema + @".object_people c
            set organisation_name = g.default_name,
            organisation_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.organisation_id = g.id ";

            Execute_OrgSQL(rec_count, 200000, sql_string, "Inserting default org data for object people");
            FeedbackResults(_schema, "object_people", "organisation_id", "organisation_ror_id");
        }
        

        // Data Object Organisations

        public void update_data_objects(bool code_all)
        {
            int rec_count = GetTableCount(_schema, "data_objects");
            RemoveInitialThes(_schema + ".data_objects", "managing_org", rec_count, 200000);
            
            string sql_string = @"update " + _schema + @".data_objects c
            set managing_org_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP          
            from " + _schema + @".temp_org_names n
            where lower(c.managing_org) = n.name
            and c.managing_org_id is null ";
            sql_string += code_all ? "" : " and coded_on is null ";

            Execute_OrgSQL(rec_count, 200000, sql_string, "Coding managing orgs for data objects");
            
            sql_string = @"update " + _schema + @".data_objects c
            set managing_org = g.default_name,
            managing_org_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.managing_org_id = g.id ";

            Execute_OrgSQL(rec_count, 200000, sql_string, "Inserting default org data for data objects");
            FeedbackResults(_schema, "data_objects", "managing_org_id", "managing_org_ror_id");
        }
        
        
        // Object instance organisations

        public void update_object_instances(bool code_all)
        {
            int rec_count = GetTableCount(_schema, "object_instances");
            RemoveInitialThes(_schema + ".object_instances", "system", rec_count, 200000);
            
            string sql_string = @"update " + _schema + @".object_instances c
            set system_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP
            from " + _schema + @".temp_org_names n
            where c.system_id is null
            and c.system is not null
            and lower(c.system) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";

            Execute_OrgSQL(rec_count, 200000, sql_string, "Coding orgs for object instances");
            
            sql_string = @"update " + _schema + @".object_instances c
            set system = g.default_name
            from context_ctx.organisations g
            where c.system_id = g.id ";

            Execute_OrgSQL(rec_count, 200000, sql_string, "Inserting default org data for object instances");
            FeedbackResults(_schema, "object_instances", "system_id", "");
        }

        private void Execute_OrgSQL(int rec_count, int rec_batch, string base_sql, string action)
        {
            try
            {
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = base_sql + " and c.id >= " + r + " and c.id < " + (r + rec_batch);
                        Execute_SQL(batch_sql_string);
                        string feedback = $"{action} - records {r} to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch).ToString() : rec_count.ToString();
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
            string sql_string = @"drop table " + _schema + @".temp_org_names;
               drop table " + _schema + @".temp_country_names;
               drop table " + _schema + @".temp_city_names ";

            Execute_SQL(sql_string);
        }

        
        // Code country names
        
        public void update_study_countries(bool code_all)
        {
            int rec_count = GetTableCount(_schema, "study_countries");
            
            string sql_string = @"update " + _schema + @".study_countries c
            set country_id = n.geoname_id,
            coded_on = CURRENT_TIMESTAMP
            from " + _schema + @".temp_country_names n
            where c.country_id is null
            and c.country_name is not null
            and lower(c.country_name) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";

            Execute_LocationSQL(rec_count, 200000, sql_string, "Coding country names");
            
            sql_string = @"update " + _schema + @".study_countries c
                        set country_name = n.country_name
                        from " + _schema + @".temp_country_names n 
                        where c.country_id = n.geoname_id ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            Execute_LocationSQL(rec_count, 200000, sql_string, "Inserting default country names");
        }
           
        
        // Code location city and country codes
        
        public void update_studylocation_orgs(bool code_all)
        {
            int rec_count = GetTableCount(_schema, "study_locations");
            
            RemoveInitialThes(_schema + ".study_locations", "facility", rec_count, 200000);
            
            string sql_string = @"update " + _schema + @".study_locations c
            set facility_org_id = n.org_id
            from " + _schema + @".temp_org_names n
            where c.facility_org_id is null
            and c.facility is not null
            and lower(c.facility) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";

            Execute_LocationSQL(rec_count, 200000, sql_string, "Coding facility names");
            
            sql_string = @"update " + _schema + @".study_locations c
            set facility = g.default_name,
            facility_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.facility_org_id = g.id ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            Execute_LocationSQL(rec_count, 200000, sql_string, "Inserting default facility names");
            
            sql_string = @"update " + _schema + @".study_locations c
            set city_id = locs.city_id,
            city_name = locs.city,
            country_id = locs.country_id,
            country_name = locs.country,
            coded_on = CURRENT_TIMESTAMP
            from context_ctx.org_locations locs
            where c.facility_org_id = locs.org_id ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            Execute_LocationSQL(rec_count, 200000, sql_string, "Added location city and country default names");
        }

        
        private void Execute_LocationSQL(int rec_count, int rec_batch, string base_sql, string action )
        {
            try
            {
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = base_sql + " and c.id >= " + r + " and c.id < " + (r + rec_batch);
                        Execute_SQL(batch_sql_string);
                        string feedback = $"{action} - records {r} to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch).ToString() : rec_count.ToString();
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
        

        private void RemoveInitialThes(string table_name, string field_name, int rec_count, int rec_batch)
        {
            string action = $"Removing initial 'The's from org names in {table_name}";
            
            string base_sql = $@"update {table_name} c
            set {field_name} = trim(substring({field_name}, 4)) 
            where {field_name} ilike 'The %'
            and cardinality(string_to_array({field_name} , ' ')) > 2";
            
            try
            {
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = base_sql + " and c.id >= " + r + " and c.id < " + (r + rec_batch);
                        Execute_SQL(batch_sql_string);
                        string feedback = $"{action} - records {r} to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch).ToString() : rec_count.ToString();
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
        

        // Store unmatched names (not used in testing)

        public int store_unmatched_study_identifiers_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id + @" and source_table = 'study_identifiers';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'study_identifiers', source, count(source) 
            from "+ _schema + @".study_identifiers 
            where source_id is null 
            group by source; ";

            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Stored {res} unmatched study identifier organisation names, for review");
            return res;
        }

        public int store_unmatched_study_organisation_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id + @" and source_table = 'study_organisations';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'study_organisations', organisation_name, count(organisation_name) 
            from " + _schema + @".study_organisations 
            where organisation_id is null 
            group by organisation_name;";

            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Stored {res} unmatched study organisation names, for review");
            return res;
        }

        public int store_unmatched_study_people_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id + @" and source_table = 'study_people';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'study_people', organisation_name, count(organisation_name) 
            from " + _schema + @".study_people 
            where organisation_id is null 
            group by organisation_name;";

            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Stored {res} unmatched study people organisation names, for review");
            return res;
        }

        public int store_unmatched_object_identifiers_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id + @" and source_table = 'object_identifiers';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'object_identifiers', source, count(source) 
            from " + _schema + @".object_identifiers 
            where source_id is null 
            group by source; ";

            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Stored {res} unmatched study identifier organisation names, for review");
            return res;
        }


        public int store_unmatched_object_organisation_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id + @" and source_table = 'object_organisations';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'object_organisations', organisation_name, count(organisation_name) 
            from " + _schema + @".object_organisations 
            where organisation_id is null 
            group by organisation_name;";

            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Stored {res} unmatched object organisation names, for review");
            return res;
        }
        
        
        public int store_unmatched_object_people_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
                                + source_id + @" and source_table = 'object_people';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'object_people', organisation_name, count(organisation_name) 
            from " + _schema + @".object_people 
            where organisation_id is null 
            group by organisation_name;";

            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Stored {res} unmatched object people organisation names, for review");
            return res;
        }
        

        public int store_unmatched_data_object_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id + @" and source_table = 'data_objects';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'data_objects', managing_org, count(managing_org) 
            from " + _schema + @".data_objects 
            where managing_org_id is null 
            group by managing_org; ";

            int res = Execute_SQL(sql_string); 
            _loggingHelper.LogLine($"Stored {res} unmatched object managing organisation names, for review");
            return res;
        }
        
        
        public int store_unmatched_object_instance_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
                                + source_id + @" and source_table = 'object_instances';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'object_instances', system, count(system) 
            from " + _schema + @".object_instances 
            where system_id is null 
            group by system; ";

            int res = Execute_SQL(sql_string); 
            _loggingHelper.LogLine($"Stored {res} unmatched object instance organisation names, for review");
            return res;
        }
        
        public int store_unmatched_country_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_countries where source_id = "
                                + source_id + @" and source_table = 'study_countries';
            insert into context_ctx.to_match_countries (source_id, source_table, country_name, number_of) 
            select " + source_id + @", 'study_countries', country_name, count(country_name) 
            from " + _schema + @".study_countries 
            where country_id is null 
            group by country_name; ";

            int res = Execute_SQL(sql_string); 
            _loggingHelper.LogLine($"Stored {res} unmatched country names, for review");
            return res;
        }
        
        
        public int store_unmatched_location_country_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_countries where source_id = "
                                + source_id + @" and source_table = 'study_locations';
            insert into context_ctx.to_match_countries (source_id, source_table, country_name, number_of) 
            select " + source_id + @", 'study_locations', country_name, count(country_name) 
            from " + _schema + @".study_locations 
            where country_id is null 
            group by country_name; ";

            int res = Execute_SQL(sql_string); 
            _loggingHelper.LogLine($"Stored {res} unmatched location country names, for review");
            return res;
        }
        
        
        public int store_unmatched_city_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_cities where source_id = "
                                + source_id + @" and source_table = 'study_locations';
            insert into context_ctx.to_match_cities (source_id, source_table, city_name, number_of) 
            select " + source_id + @", 'study_locations', city_name, count(city_name) 
            from " + _schema + @".study_locations 
            where city_id is null 
            group by city_name; ";

            int res = Execute_SQL(sql_string); 
            _loggingHelper.LogLine($"Stored {res} unmatched location city names, for review");
            return res;
        }
    }
}
