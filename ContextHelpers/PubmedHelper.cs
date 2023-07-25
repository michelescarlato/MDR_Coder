using Dapper;
using Npgsql;

namespace MDR_Coder
{
    public class PubmedHelper
    {
        private readonly string _db_conn;
        private readonly ILoggingHelper _loggingHelper;     
        private readonly string scope_qualifier;

        public PubmedHelper(Source source, ILoggingHelper logger, int scope, bool recodeTestDataOnly)
        {
            _db_conn = source.db_conn ?? "";
            _loggingHelper = logger;
            scope_qualifier = scope == 1 ? " and b.coded_on is null " : ""; 
            if (recodeTestDataOnly)
            {
                // test data 'trumps' the scope above 
                scope_qualifier = " and b.sd_sid in (select sd_sid from mn.test_study_list) ";
            }
        }
        
        private int ExecuteSQL(string sql_string)
        {
            using var conn = new NpgsqlConnection(_db_conn);
            return conn.Execute(sql_string);
        }

        public void clear_publisher_names()
        {
            // If this is 'code all' clear all the existing publisher data in the journal_details table.
            // Otherwise leave the data as is - only newly added ot test data will be coded
            
            if (scope_qualifier == "")
            {
                string sql_string = $@"update ad.journal_details jd
                            set publisher_id = null,
                               publisher = null,
                               coded_on = null";
                int res = ExecuteSQL(sql_string);
                _loggingHelper.LogLine($"Cleared {res} journal details records of publisher information");
            }
        }

        // Update publisher name in journal details using nlm id
        
        public void obtain_publisher_names()
        {
            string sql_string = $@"update ad.journal_details b
                            set publisher_id = p.publisher_id,
                            publisher = p.publisher,
                            coded_on = CURRENT_TIMESTAMP(0)
                            from context_ctx.periodicals p
                            where b.journal_nlm_id = p.nlm_unique_id {scope_qualifier}";
            int res = ExecuteSQL(sql_string);
            _loggingHelper.LogLine($"Updated {res} journal details records with publisher, using nlm ids");
        }

        // Then need to update 'managing organisation' (= publisher) using the data in
        // the journal_details table. Addition of ROR ids, if possible, will be done later.

        public void update_objects_publisher_data()
        {
            string sql_string = $@"update ad.data_objects b
                            set managing_org_id = jd.publisher_id,
                            managing_org = jd.publisher,
                            coded_on = CURRENT_TIMESTAMP(0)
                            from ad.journal_details jd
                            where b.sd_oid = jd.sd_oid {scope_qualifier}";
            int res = ExecuteSQL(sql_string);
            _loggingHelper.LogLine($"Updated {res} data object records with publisher information");
        }
        
        // Also update the publishers' identifiers in the object identifiers' table.

        public void update_identifiers_publisher_data()
        {
            string sql_string = $@"update ad.object_identifiers b
                            set source_id = jd.publisher_id,
                            source = jd.publisher,
                            coded_on = CURRENT_TIMESTAMP(0)
                            from ad.journal_details jd
                            where b.sd_oid = jd.sd_oid
                            and b.identifier_type_id = 34 {scope_qualifier}";
            int res = ExecuteSQL(sql_string);
            _loggingHelper.LogLine($"Updated {res} object identifier records with publisher information");
        }


        public int store_unmatched_publisher_org_names(int source_id)
        {
            string sql_string = $@"delete from context_ctx.to_match_orgs where source_id = {source_id} 
                                   and source_table = 'journal_details';
            insert into context_ctx.to_match_orgs (source_id, source_table, org_name, number_of) 
            select {source_id}, 'journal_details', publisher, count(publisher) 
            from ad.journal_details 
            where publisher_id is null 
            group by publisher;";
            
            return ExecuteSQL(sql_string);
        }
        
    }

}
