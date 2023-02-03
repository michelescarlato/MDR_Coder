using Dapper;
using Npgsql;

namespace MDR_Coder
{
    public class PubmedHelper
    {
        string _db_conn;
        string _schema;

        public PubmedHelper(Source source)
        {
            _db_conn = source.db_conn;
            _schema = source.source_type == "test" ? "expected" : "sd";
        }

        // update publisher name in citation object
        // using eissn code
        public void obtain_publisher_names_using_eissn()
        {
            string sql_string = @"with t as (select e.eissn, p.publisher, 
                               p.org_id, g.default_name, g.display_suffix
                               from context_ctx.pub_eissns e
                               inner join context_ctx.publishers p
                               on e.pub_id = p.id
                               inner join context_ctx.organisations g
                               on p.org_id = g.id)
                            update " + _schema + @".journal_details jd
                            set publisher_id = t.org_id,
                               publisher = t.default_name,
                               publisher_suffix = t.display_suffix
                            from t
                            where jd.eissn = t.eissn";

            using (var conn = new NpgsqlConnection(_db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // update publisher name in citation object
        // using pissn code
        public void obtain_publisher_names_using_pissn()
        {
            string sql_string = @"with t as (select e.pissn, p.publisher, 
                               p.org_id, g.default_name, g.display_suffix
                               from context_ctx.pub_pissns e
                               inner join context_ctx.publishers p
                               on e.pub_id = p.id
                               inner join context_ctx.organisations g
                               on p.org_id = g.id)
                            update " + _schema + @".journal_details jd
                            set publisher_id = t.org_id,
                               publisher = t.default_name,
                               publisher_suffix = t.display_suffix
                            from t
                            where jd.pissn = t.pissn
                            and jd.publisher_id is null";

            using (var conn = new NpgsqlConnection(_db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // update publisher name in citation object
        // using journal title, for remainer
        // (but are journal titles unique - probably not...)
        public void obtain_publisher_names_using_journal_names()
        {
            string sql_string = @"with t as (select e.journal_name, p.publisher, 
                               p.org_id, g.default_name, g.display_suffix
                               from context_ctx.pub_journals e
                               inner join context_ctx.publishers p
                               on e.pub_id = p.id
                               inner join context_ctx.organisations g
                               on p.org_id = g.id)
                            update " + _schema + @".journal_details jd
                            set publisher_id = t.org_id,
                               publisher = t.default_name,
                               publisher_suffix = t.display_suffix
                            from t
                            where lower(jd.journal_title) = lower(t.journal_name)
                            and jd.publisher_id is null"; ;

            using (var conn = new NpgsqlConnection(_db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // then need to update publisher organisation name
        // using the 'official' default name and any suffix in
        // in the organisations table.

        public void update_objects_publisher_data()
        {
            string sql_string = @"update " + _schema + @".data_objects b
                            set managing_org_id = jd.publisher_id,
                            managing_org = jd.publisher ||
                            case when jd.publisher_suffix is not null and trim(jd.publisher_suffix) <> '' 
                            then ' (' || jd.publisher_suffix || ')'
                            else '' end
                            from " + _schema + @".journal_details jd
                            where b.sd_oid = jd.sd_oid;";

            using (var conn = new NpgsqlConnection(_db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        // update the publishers' identifier ids in the
        // object identifiers' table
        // using the updated org_ids in the citation objects table

        public void update_identifiers_publisher_data()
        {
            string sql_string = @"update " + _schema + @".object_identifiers i
                            set identifier_org_id = jd.publisher_id,
                            identifier_org = jd.publisher ||
                            case when jd.publisher_suffix is not null and trim(jd.publisher_suffix) <> '' 
                            then ' (' || jd.publisher_suffix || ')'
                            else '' end
                            from " + _schema + @".journal_details jd
                            where i.sd_oid = jd.sd_oid
                            and i.identifier_type_id = 34;";

            using (var conn = new NpgsqlConnection(_db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void store_unmatched_publisher_org_names(int source_id)
        {
            string sql_string = @"delete from context_ctx.to_match_orgs where source_id = "
            + source_id.ToString() + @" and source_table = 'journal_details';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select " + source_id.ToString() + @", 'journal_details', publisher, count(publisher) 
            from " + _schema + @".journal_details 
            where publisher_id is null 
            group by publisher;";

            using (var conn = new NpgsqlConnection(_db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        /*
        public void transfer_citation_objects_to_data_objects()
        {
            string sql_string = @"insert into sd.data_objects
                        (sd_oid, sd_sid, 
                         display_title, version, doi, doi_status_id, publication_year,
                         object_class_id, object_class, object_type_id, object_type, 
                         managing_org_id, managing_org, lang_code, access_type_id, access_type,
                         access_details, access_details_url, url_last_checked, eosc_category, add_study_contribs,
                         add_study_topics, datetime_of_data_fetch)
                        SELECT 
                         sd_oid, sd_sid, 
                         display_title, version, doi, doi_status_id, publication_year,
                         object_class_id, object_class, object_type_id, object_type, 
                         managing_org_id, managing_org, lang_code, access_type_id, access_type,
                         access_details, access_details_url, url_last_checked, eosc_category, add_study_contribs,
                         add_study_topics, datetime_of_data_fetch
                        FROM sd.citation_objects;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void store_bank_links_in_pp_schema()
        {
            string sql_string = @"DROP TABLE IF EXISTS pp.bank_links;
                         CREATE TABLE pp.bank_links as
                         SELECT 
                         nlm.id as source_id, db.id_in_db as sd_sid, db.sd_oid as pmid
                         from sd.object_db_links db
                         inner join context_ctx.nlm_databanks nlm
                         on db.db_name = nlm.nlm_abbrev
                         where bank_type = 'Trial registry';";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void combine_distinct_study_pubmed_links()
        {
            string sql_string = @"DROP TABLE IF EXISTS pp.total_pubmed_links;
                        CREATE TABLE pp.total_pubmed_links as
                        SELECT source_id, sd_sid, pmid
                        FROM pp.bank_links
                        UNION
                        SELECT source_id, sd_sid, pmid
                        FROM pp.pmids_by_source_total;";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        */

    }

}
