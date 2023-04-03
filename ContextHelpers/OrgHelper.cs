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

        private void FeedbackResults(string schema, string table_name,
                                     string id_field, string ror_field)
                   
        {
            int table_count = GetTableCount(schema, table_name);
            int org_id_count = GetFieldCount(schema, table_name, id_field);
            int ror_id_count = GetFieldCount(schema, table_name, ror_field);
            _loggingHelper.LogLine($"{org_id_count} records, from {table_count}, have MDR coded organisations in {schema}.{table_name}");
            _loggingHelper.LogLine($"{ror_id_count} records, from {table_count}, have ROR coded organisations in {schema}.{table_name}");
        }
        
        private void FeedbackLocationResults(string schema, string table_name, string id_field)
                   
        {
            int table_count = GetTableCount(schema, table_name);
            int org_id_count = GetFieldCount(schema, table_name, id_field);
            _loggingHelper.LogLine($"{org_id_count} records, from {table_count}, have Geonames coded organisations in {schema}.{table_name}");
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
            string sql_string = @"update " + _schema + @".study_identifiers i
            set identifier_org_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP    
            from " + _schema + @".temp_org_names n
            where identifier_org_id is null 
            and lower(i.identifier_org) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";

            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Coded {res} organisations in study identifiers");
        
            sql_string = @"update " + _schema + @".study_identifiers i
            set identifier_org = g.default_name,
            identifier_org_ror_id = g.ror_id
            from context_ctx.organisations g
            where i.identifier_org_id = g.id ";
            
            Execute_SQL(sql_string);
            FeedbackResults(_schema, "study_identifiers", "identifier_org_id", "identifier_org_ror_id");
        }


        // Study contributor Organisations

        public void update_study_organisations(bool code_all)
        {
            string sql_string = @"update " + _schema + @".study_organisations c
            set organisation_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP    
            from " + _schema + @".temp_org_names n
            where c.organisation_id is null
            and c.organisation_name is not null
            and lower(c.organisation_name) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Coded {res} organisations in study organisations");        
            
            sql_string = @"update " + _schema + @".study_organisations c
            set organisation_name = g.default_name,
            organisation_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.organisation_id = g.id ";

            Execute_SQL(sql_string);
            FeedbackResults(_schema, "study_organisations", "organisation_id", "organisation_ror_id");
        }


        public void update_missing_sponsor_ids()
        {
            string sql_string = @"update " + _schema + @".study_identifiers si
                   set identifier_org_id = sc.organisation_id,
                   identifier_org = sc.organisation_name,
                   identifier_org_ror_id = sc.organisation_ror_id,
                   coded_on = CURRENT_TIMESTAMP    
                   from " + _schema + @".study_organisations sc
                   where si.sd_sid = sc.sd_sid
                   and (si.identifier_org ilike 'sponsor' 
                   or si.identifier_org ilike 'company internal')
                   and sc.contrib_type_id = 54 ";

            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Coded {res} additional sponsor organisations in study identifiers");
        }
        

        // Study contributor people

        public void update_study_people(bool code_all)
        {
            string sql_string = @"update " + _schema + @".study_people c
            set organisation_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP    
            from " + _schema + @".temp_org_names n
            where c.organisation_id is null
            and c.organisation_name is not null
            and lower(c.organisation_name) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Coded {res} organisations in study people");      
            
            sql_string = @"update " + _schema + @".study_people c
            set organisation_name = g.default_name,
            organisation_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.organisation_id = g.id ";

            Execute_SQL(sql_string);
            FeedbackResults(_schema, "study_people", "organisation_id", "organisation_ror_id");
        }
        

        // Object identifier Organisations

        public void update_object_identifiers(bool code_all)
        {
            string sql_string = @"update " + _schema + @".object_identifiers i
            set identifier_org_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP    
            from " + _schema + @".temp_org_names n
            where identifier_org_id is null 
            and lower(i.identifier_org) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Coded {res} organisations in object identifiers");
        
            sql_string = @"update " + _schema + @".object_identifiers i
            set identifier_org = g.default_name,
            identifier_org_ror_id = g.ror_id
            from context_ctx.organisations g
            where i.identifier_org_id = g.id ";

            Execute_SQL(sql_string);
            FeedbackResults(_schema, "object_identifiers", "identifier_org_id", "identifier_org_ror_id");
        }


        // Object contributor Organisations

        public void update_object_organisations(bool code_all)
        {
            string sql_string = @"update " + _schema + @".object_organisations c
            set organisation_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP    
            from " + _schema + @".temp_org_names n
            where c.organisation_id is null
            and c.organisation_name is not null
            and lower(c.organisation_name) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";

            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Coded {res} organisations in object organisations");
            
            sql_string = @"update " + _schema + @".object_organisations c
            set organisation_name = g.default_name,
            organisation_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.organisation_id = g.id ";

            Execute_SQL(sql_string);
            FeedbackResults(_schema, "object_organisations", "organisation_id", "organisation_ror_id");
        }

        
        // Object People

        public void update_object_people(bool code_all)
        {
            string sql_string = @"update " + _schema + @".object_people c
            set organisation_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP    
            from " + _schema + @".temp_org_names n
            where c.organisation_id is null
            and c.organisation_name is not null
            and lower(c.organisation_name) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";

            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Coded {res} organisations in object people");
            
            sql_string = @"update " + _schema + @".object_people c
            set organisation_name = g.default_name,
            organisation_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.organisation_id = g.id ";

            Execute_SQL(sql_string);
            FeedbackResults(_schema, "object_people", "organisation_id", "organisation_ror_id");
        }
        

        // Data Object Organisations

        public void update_data_objects(bool code_all)
        {
            string sql_string = @"update " + _schema + @".data_objects d
            set managing_org_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP          
            from " + _schema + @".temp_org_names n
            where lower(d.managing_org) = n.name
            and d.managing_org_id is null ";
            sql_string += code_all ? "" : " and coded_on is null ";

            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Coded {res} organisations as data object managers");
            
            sql_string = @"update " + _schema + @".data_objects d
            set managing_org = g.default_name,
            managing_org_ror_id = g.ror_id
            from context_ctx.organisations g
            where d.managing_org_id = g.id ";

            Execute_SQL(sql_string);
            FeedbackResults(_schema, "data_objects", "managing_org_id", "managing_org_ror_id");
        }
        
        
        // Object instance organisations

        public void update_object_instances(bool code_all)
        {
            string sql_string = @"update " + _schema + @".object_instances c
            set repository_org_id = n.org_id,
            coded_on = CURRENT_TIMESTAMP
            from " + _schema + @".temp_org_names n
            where c.repository_org_id is null
            and c.repository_org is not null
            and lower(c.repository_org) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";

            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Coded {res} organisations in object instances");
            
            sql_string = @"update " + _schema + @".object_instances c
            set repository_org = g.default_name,
            repository_org_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.repository_org_id = g.id ";

            Execute_SQL(sql_string);
            FeedbackResults(_schema, "object_instances", "repository_org_id", "repository_org_ror_id");
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
            string sql_string = @"update " + _schema + @".study_countries c
            set country_id = n.geoname_id,
            coded_on = CURRENT_TIMESTAMP
            from " + _schema + @".temp_country_names n
            where c.country_id is null
            and c.country_name is not null
            and lower(c.country_name) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Coded {res} country names");
            
            sql_string = @"update " + _schema + @".study_countries c
            set country_name = n.country_name
            from " + _schema + @".temp_country_names n 
            where c.country_id = n.geoname_id ";

            Execute_SQL(sql_string);
            FeedbackLocationResults(_schema, "study_countries", "country_id");
        }
        
        
        // Code location city and country codes
        
        public void update_studylocation_cities(bool code_all)
        {
            string sql_string = @"update " + _schema + @".study_locations c
            set city_id = n.geoname_id,
            coded_on = CURRENT_TIMESTAMP
            from " + _schema + @".temp_city_names n
            where c.city_id is null
            and c.city_name is not null
            and lower(c.city_name) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Coded {res} location city and country names");
            
            sql_string = @"update " + _schema + @".study_locations c
            set city_name = n.city_name,
            country_id = n.country_id,
            country_name = n.country_name
            from " + _schema + @".temp_city_names n 
            where c.city_id = n.geoname_id ";
            
            Execute_SQL(sql_string);
            FeedbackLocationResults(_schema, "study_locations", "city_id");
        }
                
        public void update_studylocation_countries(bool code_all)
        {
            string sql_string = @"update " + _schema + @".study_locations c
            set country_id = n.geoname_id,
            coded_on = CURRENT_TIMESTAMP
            from " + _schema + @".temp_country_names n
            where c.country_id is null
            and c.country_name is not null
            and lower(c.country_name) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            int res = Execute_SQL(sql_string);
            _loggingHelper.LogLine($"Coded {res} additional location country names");
            
            sql_string = @"update " + _schema + @".study_countries c
            set country_name = n.country_name
            from " + _schema + @".temp_country_names n 
            where c.country_id = n.geoname_id ";

            Execute_SQL(sql_string);
            FeedbackLocationResults(_schema, "study_locations", "country_id");
        }
        
        

        // Store unmatched names (not used in testing)

        public int store_unmatched_study_identifiers_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id + @" and source_table = 'study_identifiers';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'study_identifiers', identifier_org, count(identifier_org) 
            from "+ _schema + @".study_identifiers 
            where identifier_org_id is null 
            group by identifier_org; ";

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
            select " + source_id + @", 'object_identifiers', identifier_org, count(identifier_org) 
            from " + _schema + @".object_identifiers 
            where identifier_org_id is null 
            group by identifier_org; ";

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
            select " + source_id + @", 'object_instances', repository_org, count(repository_org) 
            from " + _schema + @".object_instances 
            where repository_org_id is null 
            group by repository_org; ";

            int res = Execute_SQL(sql_string); 
            _loggingHelper.LogLine($"Stored {res} unmatched object instance organisation names, for review");
            return res;
        }
        
        public int store_unmatched_country_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_countries where source_id = "
                                + source_id + @" and source_table = 'study_countries';
            insert into context_ctx.to_match_countries (source_id, source_table, country_name, number_of) 
            select " + source_id + @", 'study_countries', country_name, count(source_id) 
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
            select " + source_id + @", 'study_locations', country_name, count(source_id) 
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
            insert into context_ctx.to_match_orgs (source_id, source_table, city_name, number_of) 
            select " + source_id + @", 'study_locations', city_name, count(source_id) 
            from " + _schema + @".study_locations 
            where city_id is null 
            group by city_name; ";

            int res = Execute_SQL(sql_string); 
            _loggingHelper.LogLine($"Stored {res} unmatched location city names, for review");
            return res;
        }
    }
}
