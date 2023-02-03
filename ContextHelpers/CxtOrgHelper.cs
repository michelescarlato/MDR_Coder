using Dapper;
using Npgsql;

namespace MDR_Coder
{
    public class OrgHelper
    {
        string _db_conn;
        string _schema;

        public OrgHelper(Source source)
        {
            _db_conn = source.db_conn;
            _schema = source.source_type == "test" ? "expected" : "sd"; 
        }


        public void Execute_SQL(string sql_string)
        {
            using var conn = new NpgsqlConnection(_db_conn);
            conn.Execute(sql_string);
        }

        // Set up relevant names for comparison
        public void establish_temp_names_table()
        {
            string sql_string = @"drop table if exists " + _schema + @".temp_org_names;
                 create table " + _schema + @".temp_org_names 
                 as 
                 select a.org_id, lower(a.name) as name from 
                 context_ctx.org_names a
                 inner join context_ctx.organisations g
                 on a.org_id = g.id
                 where a.qualifier_id <> 10";

            Execute_SQL(sql_string);
        }


        // Study identifier Organisations

        public void update_study_identifiers_using_names()
        {
            string sql_string = @"update " + _schema + @".study_identifiers i
            set identifier_org_id = n.org_id
            from " + _schema + @".temp_org_names n
            where identifier_org_id is null 
            and lower(i.identifier_org) = n.name;";

            Execute_SQL(sql_string);
        }

        // need to check the possible utility of this....as an additional option

        public void update_study_identifiers_using_default_name_and_suffix()
        {
            string sql_string = @"update " + _schema + @".study_identifiers i
            set identifier_org_id = g.id
            from context_ctx.organisations g
            where g.display_suffix is not null and g.trim(display_suffix) <> '' 
            and lower(i.identifier_org) =  lower(g.default_name || ' (' || g.display_suffix || ')') 
            and identifier_org_id is null;";

            Execute_SQL(sql_string);
        }


        public void update_study_identifiers_insert_default_names()
        {
            string sql_string = @"update " + _schema + @".study_identifiers i
            set identifier_org = g.default_name ||
            case when g.display_suffix is not null and trim(g.display_suffix) <> '' then ' (' || g.display_suffix || ')'
            else '' end,
            identifier_org_ror_id = g.ror_id
            from context_ctx.organisations g
            where i.identifier_org_id = g.id;";

            Execute_SQL(sql_string);
        }


        // Study contributor Organisations

        public void update_study_contributors_using_names()
        {
            string sql_string = @"update " + _schema + @".study_contributors c
            set organisation_id = n.org_id
            from " + _schema + @".temp_org_names n
            where c.organisation_id is null
            and c.organisation_name is not null
            and lower(c.organisation_name) = n.name;";

            Execute_SQL(sql_string);
        }

        public void update_study_contributors_insert_default_names()
        {
            string sql_string = @"update " + _schema + @".study_contributors c
            set organisation_name = g.default_name ||
            case when g.display_suffix is not null and trim(g.display_suffix) <> '' then ' (' || g.display_suffix || ')'
            else '' end,
            organisation_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.organisation_id = g.id;";

            Execute_SQL(sql_string);
        }


        public void update_missing_sponsor_ids()
        {
            string sql_string = @"update " + _schema + @".study_identifiers si
                   set identifier_org_id = sc.organisation_id,
                   identifier_org = sc.organisation_name,
                   identifier_org_ror_id = sc.organisation_ror_id
                   from " + _schema + @".study_contributors sc
                   where si.sd_sid = sc.sd_sid
                   and (si.identifier_org ilike 'sponsor' 
                   or si.identifier_org ilike 'company internal')
                   and sc.contrib_type_id = 54";

            Execute_SQL(sql_string);
        }


        // Object identifier Organisations

        public void update_object_identifiers_using_names()
        {
            string sql_string = @"update " + _schema + @".object_identifiers i
            set identifier_org_id = n.org_id
            from " + _schema + @".temp_org_names n
            where identifier_org_id is null 
            and lower(i.identifier_org) = n.name;";

            Execute_SQL(sql_string);
        }

        
        public void update_object_identifiers_insert_default_names()
        {
            string sql_string = @"update " + _schema + @".object_identifiers i
            set identifier_org = g.default_name ||
            case when g.display_suffix is not null and trim(g.display_suffix) <> '' then ' (' || g.display_suffix || ')'
            else '' end,
            identifier_org_ror_id = g.ror_id
            from context_ctx.organisations g
            where i.identifier_org_id = g.id;";

            Execute_SQL(sql_string);
        }


        // Object contributor Organisations

        public void update_object_contributors_using_names()
        {
            string sql_string = @"update " + _schema + @".object_contributors c
            set organisation_id = n.org_id
            from " + _schema + @".temp_org_names n
            where c.organisation_id is null
            and c.organisation_name is not null
            and lower(c.organisation_name) = n.name;";

            Execute_SQL(sql_string);
        }

        public void update_object_contributors_insert_default_names()
        {
            string sql_string = @"update " + _schema + @".object_contributors c
            set organisation_name = g.default_name ||
            case when g.display_suffix is not null and trim(g.display_suffix) <> '' then ' (' || g.display_suffix || ')'
            else '' end,
            organisation_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.organisation_id = g.id;";

            Execute_SQL(sql_string);
        }


        // Data Object Organisations

        public void update_data_objects_using_names()
        {
            string sql_string = @"update " + _schema + @".data_objects d
            set managing_org_id = n.org_id           
            from " + _schema + @".temp_org_names n
            where lower(d.managing_org) = n.name
            and d.managing_org_id is null;";

            Execute_SQL(sql_string);
        }


        public void update_data_objects_insert_default_names()
        {
            string sql_string = @"update " + _schema + @".data_objects d
            set managing_org = g.default_name ||
            case when g.display_suffix is not null and trim(g.display_suffix) <> '' then ' (' || g.display_suffix || ')'
            else '' end,
            managing_org_ror_id = g.ror_id
            from context_ctx.organisations g
            where d.managing_org_id = g.id;";

            Execute_SQL(sql_string);
        }



        public void delete_temp_names_table()
        {
            string sql_string = @"drop table " + _schema + @".temp_org_names;";

            Execute_SQL(sql_string);
        }


        // Store unmatched names (not used in testing)

        public void store_unmatched_study_identifiers_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id.ToString() + @" and source_table = 'study_identifiers';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id.ToString() + @", 'study_identifiers', identifier_org, count(identifier_org) 
            from "+ _schema + @".study_identifiers 
            where identifier_org_id is null 
            group by identifier_org; ";

            Execute_SQL(sql_string);

        }

        public void store_unmatched_study_contributors_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id.ToString() + @" and source_table = 'study_contributors';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id.ToString() + @", 'study_contributors', organisation_name, count(organisation_name) 
            from " + _schema + @".study_contributors 
            where organisation_id is null 
            group by organisation_name;";

            Execute_SQL(sql_string);

        }


        public void store_unmatched_object_identifiers_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id.ToString() + @" and source_table = 'object_identifiers';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id.ToString() + @", 'object_identifiers', identifier_org, count(identifier_org) 
            from " + _schema + @".object_identifiers 
            where identifier_org_id is null 
            group by identifier_org; ";

            Execute_SQL(sql_string);

        }


        public void store_unmatched_object_contributors_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id.ToString() + @" and source_table = 'object_contributors';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id.ToString() + @", 'object_contributors', organisation_name, count(organisation_name) 
            from " + _schema + @".object_contributors 
            where organisation_id is null 
            group by organisation_name;";

            Execute_SQL(sql_string);

        }

        public void store_unmatched_data_object_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id.ToString() + @" and source_table = 'data_objects';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id.ToString() + @", 'data_objects', managing_org, count(managing_org) 
            from " + _schema + @".data_objects 
            where managing_org_id is null 
            group by managing_org; ";

            Execute_SQL(sql_string);

        }
    }
}
