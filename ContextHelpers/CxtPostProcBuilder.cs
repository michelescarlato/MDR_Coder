using Dapper;
using Npgsql;

namespace MDR_Coder
{
    public class PostProcBuilder
    {
        private string connString;
        private Source _source;
        private int source_id;
        private OrgHelper org_helper;
        private PubmedHelper pubmed_helper;
        private TopicHelper topic_helper;
        private ILoggingHelper _logging_helper;

        public PostProcBuilder(Source source, ILoggingHelper logging_helper)
        {
            _source = source;
            connString = source.db_conn;
            source_id = source.id ?? 0;
            org_helper = new OrgHelper(source);
            pubmed_helper = new PubmedHelper(source);
            _logging_helper = logging_helper;
            
            topic_helper = new TopicHelper(source, _logging_helper);
        }


        public void EstablishContextForeignTables(Credentials creds)
        {
            string schema = _source.source_type == "test" ? "expected" : "sd";

            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = @"CREATE EXTENSION IF NOT EXISTS postgres_fdw
                                     schema " + schema;
                conn.Execute(sql_string);

                sql_string = @"CREATE SERVER IF NOT EXISTS context "
                           + @" FOREIGN DATA WRAPPER postgres_fdw
                             OPTIONS (host 'localhost', dbname 'context', port '5432');";
                conn.Execute(sql_string);

                sql_string = @"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                     SERVER context 
                     OPTIONS (user '" + creds.Username + "', password '" + creds.Password + "');";
                conn.Execute(sql_string);

                sql_string = @"DROP SCHEMA IF EXISTS context_ctx cascade;
                     CREATE SCHEMA context_ctx; 
                     IMPORT FOREIGN SCHEMA ctx
                     FROM SERVER context 
                     INTO context_ctx;";
                conn.Execute(sql_string);

                sql_string = @"DROP SCHEMA IF EXISTS context_lup cascade;
                     CREATE SCHEMA context_lup; 
                     IMPORT FOREIGN SCHEMA lup
                     FROM SERVER context 
                     INTO context_lup;";
                conn.Execute(sql_string);
            }
        }


        public void EstablishTempNamesTable()
        {
            org_helper.establish_temp_names_table();
        }

        public void ObtainPublisherInformation()
        {
            pubmed_helper.obtain_publisher_names_using_eissn();
            pubmed_helper.obtain_publisher_names_using_pissn();
            pubmed_helper.obtain_publisher_names_using_journal_names();
        }


        public void ApplyPublisherData()
        {
            pubmed_helper.update_objects_publisher_data();
            pubmed_helper.update_identifiers_publisher_data();
            pubmed_helper.store_unmatched_publisher_org_names(source_id);
        }


        public void UpdateStudyIdentifierOrgs()
        {
            org_helper.update_study_identifiers_using_names();
            org_helper.update_study_identifiers_insert_default_names();
        }


        public void UpdateStudyContributorOrgs()
        {
            if (_source.has_study_contributors is true)
            {
                org_helper.update_study_contributors_using_names();
                org_helper.update_study_contributors_insert_default_names();
                org_helper.update_missing_sponsor_ids();
            }
        }


        public void StoreUnMatchedNamesForStudies()
        {
            if (_source.source_type != "test")
            {
                org_helper.store_unmatched_study_identifiers_org_names(source_id);
                if (_source.has_study_contributors is true)
                {
                    org_helper.store_unmatched_study_contributors_org_names(source_id);
                }
            }
        }


        public void UpdateObjectIdentifierOrgs()
        {
            org_helper.update_object_identifiers_using_names();
            org_helper.update_object_identifiers_insert_default_names();
        }


        public void UpdateObjectContributorOrgs()
        {
            org_helper.update_object_contributors_using_names();
            org_helper.update_object_contributors_insert_default_names();

        }


        public void StoreUnMatchedNamesForObjects()
        {
            if (_source.source_type != "test")
            {
                org_helper.store_unmatched_object_identifiers_org_names(source_id);
                org_helper.store_unmatched_object_contributors_org_names(source_id);
            }
        }


        public void UpdateDataObjectOrgs()
        {
            org_helper.update_data_objects_using_names();
            org_helper.update_data_objects_insert_default_names();
        }


        public void StoreUnMatchedNamesForDataObjects()
        {
            if (_source.source_type != "test")
            {
                org_helper.store_unmatched_data_object_org_names(source_id);
            }
        }

        public void UpdateTopics(string source_type)
        {
            if ((_source.source_type == "study" && _source.has_study_topics is true)
                 || source_type != "study")
            {
                topic_helper.process_topics();

                if (_source.source_type != "test")
                {
                    topic_helper.store_unmatched_topic_values(source_type, source_id);
                }
            }
        }

        public void DropTempNamesTable()
        {
            org_helper.delete_temp_names_table();
        }

        public void DropContextForeignTables()
        {
            using var conn = new NpgsqlConnection(connString);
            string sql_string = @"DROP USER MAPPING IF EXISTS FOR CURRENT_USER
                     SERVER context;";
            conn.Execute(sql_string);

            sql_string = @"DROP SERVER IF EXISTS context CASCADE;";
            conn.Execute(sql_string);

            sql_string = @"DROP SCHEMA IF EXISTS context_ctx;";
            conn.Execute(sql_string);

            sql_string = @"DROP SCHEMA IF EXISTS context_lup;";
            conn.Execute(sql_string);
        }

    }
}

