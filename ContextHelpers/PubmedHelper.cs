using Dapper;
using Npgsql;

namespace MDR_Coder
{
    public class PubmedHelper
    {
        private readonly string _db_conn;
        private readonly string _schema;

        public PubmedHelper(Source source)
        {
            _db_conn = source.db_conn ?? "";
            _schema = source.source_type == "test" ? "expected" : "ad";
        }
        
        
        private void Execute_SQL(string sql_string)
        {
            using var conn = new NpgsqlConnection(_db_conn);
            conn.Execute(sql_string);
        }

        public void clear_publisher_names(bool code_all)
        {
            // If this is 'code all' clear all the existing publisher data in the journal_details table.
            // Otherwise leave the data as is - only newly added data will be coded
            
            if (code_all)
            {
                string sql_string = $@"update {_schema}.journal_details jd
                            set publisher_id = null,
                               publisher = null,
                               publisher_suffix = null,
                               coded_on = null";
                Execute_SQL(sql_string);
            }
        }

        // Update publisher name in journal details using eissn code.
        
        public void obtain_publisher_names_using_eissn(bool code_all)
        {
            string sql_string = $@"with t as (select e.eissn, p.publisher, 
                               p.org_id, g.default_name
                               from context_ctx.pub_eissns e
                               inner join context_ctx.publishers p
                               on e.pub_id = p.id
                               inner join context_ctx.organisations g
                               on p.org_id = g.id)
                            update {_schema}.journal_details jd
                            set publisher_id = t.org_id,
                               publisher = t.default_name,
                               coded_on = CURRENT_TIMESTAMP(0)
                            from t
                            where jd.eissn = t.eissn ";

            sql_string += !code_all ? " and jd.coded_on is null;" : "";
            Execute_SQL(sql_string);
        }


        // Update publisher name in journal details using pissn code.
        
        public void obtain_publisher_names_using_pissn(bool code_all)
        {
            string sql_string = $@"with t as (select e.pissn, p.publisher, 
                               p.org_id, g.default_name
                               from context_ctx.pub_pissns e
                               inner join context_ctx.publishers p
                               on e.pub_id = p.id
                               inner join context_ctx.organisations g
                               on p.org_id = g.id)
                            update {_schema}.journal_details jd
                            set publisher_id = t.org_id,
                               publisher = t.default_name,
                               coded_on = CURRENT_TIMESTAMP(0)
                            from t
                            where jd.pissn = t.pissn
                            and jd.publisher_id is null ";
            
            sql_string += !code_all ? " and jd.coded_on is null;" : "";
            Execute_SQL(sql_string);
        }


        // Update publisher name in journal details using journal title, for remainder 
        // (but slight risk here as journal titles may not be unique).
        
        public void obtain_publisher_names_using_journal_names(bool code_all)
        {
            string sql_string = $@"with t as (select e.journal_name, p.publisher, 
                               p.org_id, g.default_name
                               from context_ctx.pub_journals e
                               inner join context_ctx.publishers p
                               on e.pub_id = p.id
                               inner join context_ctx.organisations g
                               on p.org_id = g.id)
                            update {_schema}.journal_details jd
                            set publisher_id = t.org_id,
                               publisher = t.default_name,
                               coded_on = CURRENT_TIMESTAMP(0)
                            from t
                            where lower(jd.journal_title) = lower(t.journal_name)
                            and jd.publisher_id is null ";

            sql_string += !code_all ? " and jd.coded_on is null;" : "";
            Execute_SQL(sql_string);
        }


        // Then need to update 'managing organisation' (= publisher) using the data in
        // the journal_details table. Addition of ROR ids, if possible, will be done later.

        public void update_objects_publisher_data(bool code_all)
        {
            string sql_string = $@"update {_schema}.data_objects b
                            set managing_org_id = jd.publisher_id,
                            managing_org = jd.publisher,
                            coded_on = CURRENT_TIMESTAMP(0)
                            from {_schema}.journal_details jd
                            where b.sd_oid = jd.sd_oid ";
            
            sql_string += !code_all ? " and b.coded_on is null;" : "";
            Execute_SQL(sql_string);
        }


        // Also update the publishers' identifiers in the object identifiers' table.

        public void update_identifiers_publisher_data(bool code_all)
        {
            string sql_string = $@"update {_schema}.object_identifiers i
                            set identifier_org_id = jd.publisher_id,
                            identifier_org = jd.publisher ||
                            case when jd.publisher_suffix is not null and trim(jd.publisher_suffix) <> '' 
                            then ' (' || jd.publisher_suffix || ')'
                            else '' end,
                            coded_on = CURRENT_TIMESTAMP(0)
                            from {_schema}.journal_details jd
                            where i.sd_oid = jd.sd_oid
                            and i.identifier_type_id = 34 ";
            
            sql_string += !code_all ? " and i.coded_on is null;" : "";
            Execute_SQL(sql_string);
        }


        public void store_unmatched_publisher_org_names(int source_id)
        {
            string sql_string = $@"delete from context_ctx.to_match_orgs where source_id = {source_id} 
                                   and source_table = 'journal_details';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select {source_id}, 'journal_details', publisher, count(publisher) 
            from {_schema}.journal_details 
            where publisher_id is null 
            group by publisher;";
            
            Execute_SQL(sql_string);
        }


        /*
         
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

                        Execute_SQL(sql_string);
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

                       Execute_SQL(sql_string);
        }

        */

    }

}
