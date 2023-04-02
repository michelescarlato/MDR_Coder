using Dapper;
using Npgsql;

namespace MDR_Coder
{
    public class OrgHelper
    {
        private readonly string _db_conn;
        private readonly string _schema;

        public OrgHelper(Source source)
        {
            _db_conn = source.db_conn ?? "";
            _schema = source.source_type == "test" ? "expected" : "ad"; 
        }

        public void Execute_SQL(string sql_string)
        {
            using var conn = new NpgsqlConnection(_db_conn);
            conn.Execute(sql_string);
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

        public void update_study_identifiers_using_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".study_identifiers i
            set identifier_org_id = n.org_id
            from " + _schema + @".temp_org_names n
            where identifier_org_id is null 
            and lower(i.identifier_org) = n.name;";

            Execute_SQL(sql_string);
        }

        
        public void update_study_identifiers_insert_default_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".study_identifiers i
            set identifier_org = g.default_name,
            identifier_org_ror_id = g.ror_id
            from context_ctx.organisations g
            where i.identifier_org_id = g.id;";

            Execute_SQL(sql_string);
        }


        // Study contributor Organisations

        public void update_study_organisations_using_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".study_organisations c
            set organisation_id = n.org_id
            from " + _schema + @".temp_org_names n
            where c.organisation_id is null
            and c.organisation_name is not null
            and lower(c.organisation_name) = n.name;";

            Execute_SQL(sql_string);
        }

        public void update_study_organisations_insert_default_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".study_organisations c
            set organisation_name = g.default_name,
            organisation_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.organisation_id = g.id;";

            Execute_SQL(sql_string);
        }


        public void update_missing_sponsor_ids(bool code_all)
        {
            string sql_string = @"update " + _schema + @".study_identifiers si
                   set identifier_org_id = sc.organisation_id,
                   identifier_org = sc.organisation_name,
                   identifier_org_ror_id = sc.organisation_ror_id
                   from " + _schema + @".study_organisations sc
                   where si.sd_sid = sc.sd_sid
                   and (si.identifier_org ilike 'sponsor' 
                   or si.identifier_org ilike 'company internal')
                   and sc.contrib_type_id = 54";

            Execute_SQL(sql_string);
        }
        

        // Study contributor people

        public void update_study_people_using_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".study_people c
            set organisation_id = n.org_id
            from " + _schema + @".temp_org_names n
            where c.organisation_id is null
            and c.organisation_name is not null
            and lower(c.organisation_name) = n.name;";

            Execute_SQL(sql_string);
        }

        public void update_study_people_insert_default_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".study_people c
            set organisation_name = g.default_name,
            organisation_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.organisation_id = g.id;";

            Execute_SQL(sql_string);
        }

        

        // Object identifier Organisations

        public void update_object_identifiers_using_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".object_identifiers i
            set identifier_org_id = n.org_id
            from " + _schema + @".temp_org_names n
            where identifier_org_id is null 
            and lower(i.identifier_org) = n.name;";

            Execute_SQL(sql_string);
        }

        
        public void update_object_identifiers_insert_default_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".object_identifiers i
            set identifier_org = g.default_name,
            identifier_org_ror_id = g.ror_id
            from context_ctx.organisations g
            where i.identifier_org_id = g.id;";

            Execute_SQL(sql_string);
        }


        // Object contributor Organisations

        public void update_object_organisations_using_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".object_organisations c
            set organisation_id = n.org_id
            from " + _schema + @".temp_org_names n
            where c.organisation_id is null
            and c.organisation_name is not null
            and lower(c.organisation_name) = n.name;";

            Execute_SQL(sql_string);
        }

        public void update_object_organisations_insert_default_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".object_organisations c
            set organisation_name = g.default_name,
            organisation_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.organisation_id = g.id;";

            Execute_SQL(sql_string);
        }

        
        // Object contributor People

        public void update_object_people_using_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".object_people c
            set organisation_id = n.org_id
            from " + _schema + @".temp_org_names n
            where c.organisation_id is null
            and c.organisation_name is not null
            and lower(c.organisation_name) = n.name;";

            Execute_SQL(sql_string);
        }

        public void update_object_people_insert_default_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".object_people c
            set organisation_name = g.default_name,
            organisation_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.organisation_id = g.id;";

            Execute_SQL(sql_string);
        }
        

        // Data Object Organisations

        public void update_data_objects_using_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".data_objects d
            set managing_org_id = n.org_id           
            from " + _schema + @".temp_org_names n
            where lower(d.managing_org) = n.name
            and d.managing_org_id is null;";

            Execute_SQL(sql_string);
        }


        public void update_data_objects_insert_default_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".data_objects d
            set managing_org = g.default_name,
            managing_org_ror_id = g.ror_id
            from context_ctx.organisations g
            where d.managing_org_id = g.id;";

            Execute_SQL(sql_string);
        }
        
        
        // Object instance organisations

        public void update_object_instances_using_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".object_instances c
            set repository_org_id = n.org_id
            from " + _schema + @".temp_org_names n
            where c.repository_org_id is null
            and c.repository_org is not null
            and lower(c.repository_org) = n.name;";

            Execute_SQL(sql_string);
        }

        public void update_object_instances_insert_default_names(bool code_all)
        {
            string sql_string = @"update " + _schema + @".object_instances c
            set repository_org = g.default_name,
            repository_org_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.repository_org_id = g.id;";

            Execute_SQL(sql_string);
        }

        public void delete_temp_tables()
        {
            string sql_string = @"drop table " + _schema + @".temp_org_names;
               drop table " + _schema + @".temp_country_names;
               drop table " + _schema + @".temp_city_names ";

            Execute_SQL(sql_string);
        }

        
        // Code country names
        
        public void update_study_countries_coding(bool code_all)
        {
            string sql_string = @"update " + _schema + @".study_countries c
            set country_id = n.geoname_id
            from " + _schema + @".temp_country_names n
            where c.country_id is null
            and c.country_name is not null
            and lower(c.country_name) = n.name;";

            Execute_SQL(sql_string);
            
            sql_string = @"update " + _schema + @".study_countries c
            set country_name = n.country_name
            from " + _schema + @".temp_country_names n 
            where c.country_id = n.geoname_id;";

            Execute_SQL(sql_string);
        }
        
        
        // Code location city and country codes
        
        public void update_studylocation_cities_coding(bool code_all)
        {
            string sql_string = @"update " + _schema + @".study_locations c
            set city_id = n.geoname_id
            from " + _schema + @".temp_city_names n
            where c.city_id is null
            and c.city_name is not null
            and lower(c.city_name) = n.name;";

            Execute_SQL(sql_string);
            
            sql_string = @"update " + _schema + @".study_locations c
            set city_name = n.city_name,
            country_id = n.country_id,
            country_name = n.country_name
            from " + _schema + @".temp_city_names n 
            where c.city_id = n.geoname_id;";
            
            Execute_SQL(sql_string);
        }
                
        public void update_studylocation_countries_coding(bool code_all)
        {
            string sql_string = @"update " + _schema + @".study_locations c
            set country_id = n.geoname_id
            from " + _schema + @".temp_country_names n
            where c.country_id is null
            and c.country_name is not null
            and lower(c.country_name) = n.name;";

            Execute_SQL(sql_string);
            
            sql_string = @"update " + _schema + @".study_countries c
            set country_name = n.country_name
            from " + _schema + @".temp_country_names n 
            where c.country_id = n.geoname_id;";

            Execute_SQL(sql_string);
        }
        
        

        // Store unmatched names (not used in testing)

        public void store_unmatched_study_identifiers_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id + @" and source_table = 'study_identifiers';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'study_identifiers', identifier_org, count(identifier_org) 
            from "+ _schema + @".study_identifiers 
            where identifier_org_id is null 
            group by identifier_org; ";

            Execute_SQL(sql_string);
        }

        public void store_unmatched_study_organisation_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id + @" and source_table = 'study_organisations';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'study_organisations', organisation_name, count(organisation_name) 
            from " + _schema + @".study_organisations 
            where organisation_id is null 
            group by organisation_name;";

            Execute_SQL(sql_string);
        }

        public void store_unmatched_study_people_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id + @" and source_table = 'study_people';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'study_people', organisation_name, count(organisation_name) 
            from " + _schema + @".study_people 
            where organisation_id is null 
            group by organisation_name;";

            Execute_SQL(sql_string);
        }

        public void store_unmatched_object_identifiers_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id + @" and source_table = 'object_identifiers';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'object_identifiers', identifier_org, count(identifier_org) 
            from " + _schema + @".object_identifiers 
            where identifier_org_id is null 
            group by identifier_org; ";

            Execute_SQL(sql_string);
        }


        public void store_unmatched_object_organisation_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id + @" and source_table = 'object_organisations';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'object_organisations', organisation_name, count(organisation_name) 
            from " + _schema + @".object_organisations 
            where organisation_id is null 
            group by organisation_name;";

            Execute_SQL(sql_string);
        }
        
        
        public void store_unmatched_object_people_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
                                + source_id + @" and source_table = 'object_people';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'object_people', organisation_name, count(organisation_name) 
            from " + _schema + @".object_people 
            where organisation_id is null 
            group by organisation_name;";

            Execute_SQL(sql_string);
        }
        

        public void store_unmatched_data_object_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id + @" and source_table = 'data_objects';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'data_objects', managing_org, count(managing_org) 
            from " + _schema + @".data_objects 
            where managing_org_id is null 
            group by managing_org; ";

            Execute_SQL(sql_string); 
        }
        
        
        public void store_unmatched_object_instance_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
                                + source_id + @" and source_table = 'object_instances';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id + @", 'object_instances', repository_org, count(repository_org) 
            from " + _schema + @".object_instances 
            where repository_org_id is null 
            group by repository_org; ";

            Execute_SQL(sql_string); 
        }
        
        public void store_unmatched_country_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_countries where source_id = "
                                + source_id + @" and source_table = 'study_countries';
            insert into context_ctx.to_match_countries (source_id, source_table, country_name, number_of) 
            select " + source_id + @", 'study_countries', country_name, count(source_id) 
            from " + _schema + @".study_countries 
            where country_id is null 
            group by country_name; ";

            Execute_SQL(sql_string); 
        }
        
        
        public void store_unmatched_location_country_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_countries where source_id = "
                                + source_id + @" and source_table = 'study_locations';
            insert into context_ctx.to_match_countries (source_id, source_table, country_name, number_of) 
            select " + source_id + @", 'study_locations', country_name, count(source_id) 
            from " + _schema + @".study_locations 
            where country_id is null 
            group by country_name; ";

            Execute_SQL(sql_string); 
        }
        
        
        public void store_unmatched_city_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_cities where source_id = "
                                + source_id + @" and source_table = 'study_locations';
            insert into context_ctx.to_match_orgs (source_id, source_table, city_name, number_of) 
            select " + source_id + @", 'study_locations', city_name, count(source_id) 
            from " + _schema + @".study_locations 
            where city_id is null 
            group by city_name; ";

            Execute_SQL(sql_string); 
        }
    }
}
