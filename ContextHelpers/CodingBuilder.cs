using System.Runtime.InteropServices.ComTypes;
using Dapper;
using Npgsql;

namespace MDR_Coder
{
    public class CodingBuilder
    {
        private readonly string connString;
        private readonly Source _source;
        private readonly Options _opts;
        private readonly int source_id;
        
        private readonly OrgHelper org_helper;
        private readonly PubmedHelper pubmed_helper;
        private readonly TopicHelper topic_helper;

        public CodingBuilder(Source source, Options opts, ILoggingHelper logging_helper)
        {
            _source = source;
            connString = source.db_conn ?? "";
            source_id = source.id ?? 0;
            _opts = opts;
            
            pubmed_helper = new PubmedHelper(source);
            org_helper = new OrgHelper(source);
            topic_helper = new TopicHelper(source, logging_helper);
        }


        public void EstablishContextForeignTables(Credentials creds)
        {
            string schema = _source.source_type == "test" ? "expected" : "sd";
            using var conn = new NpgsqlConnection(connString);
            
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


        public void EstablishTempTables()
        {
            org_helper.establish_temp_tables();
        }

        
        public void ObtainPublisherInformation()
        {
            pubmed_helper.clear_publisher_names(_opts.RecodeAllPublishers);
            pubmed_helper.obtain_publisher_names_using_eissn(_opts.RecodeAllPublishers);
            pubmed_helper.obtain_publisher_names_using_pissn(_opts.RecodeAllPublishers);
            pubmed_helper.obtain_publisher_names_using_journal_names(_opts.RecodeAllPublishers);
        }


        public void ApplyPublisherData()
        {
            pubmed_helper.update_objects_publisher_data(_opts.RecodeAllPublishers);
            pubmed_helper.update_identifiers_publisher_data(_opts.RecodeAllPublishers);
            if (_source.source_type != "test")
            {
                pubmed_helper.store_unmatched_publisher_org_names(source_id);
            }
        }


        public void UpdateStudyIdentifiers()
        {
            org_helper.update_study_identifiers_using_names(_opts.RecodeAllOrgs);
            org_helper.update_study_identifiers_insert_default_names(_opts.RecodeAllOrgs);
        }


        public void UpdateStudyOrgs()
        {
            org_helper.update_study_organisations_using_names(_opts.RecodeAllOrgs);
            org_helper.update_study_organisations_insert_default_names(_opts.RecodeAllOrgs);
            org_helper.update_missing_sponsor_ids(_opts.RecodeAllOrgs);
        }

        public void UpdateStudyPeople()
        {
            org_helper.update_study_people_using_names(_opts.RecodeAllOrgs);
            org_helper.update_study_people_insert_default_names(_opts.RecodeAllOrgs);
        }

        public void StoreUnMatchedOrgNamesForStudies()
        {
            if (_source.source_type != "test")
            {
                org_helper.store_unmatched_study_identifiers_org_names(source_id);
                if (_source.has_study_organisations is true)
                {
                    org_helper.store_unmatched_study_organisation_names(source_id);
                }
                if (_source.has_study_people is true)
                {
                    org_helper.store_unmatched_study_people_org_names(source_id);
                }
            }
        }
        
        public void UpdateStudyCountries()
        {
            org_helper.update_study_countries_coding(_opts.RecodeAllLocations);
        }
        
        public void UpdateStudyLocations()
        {
            org_helper.update_studylocation_cities_coding(_opts.RecodeAllLocations);            
            org_helper.update_studylocation_countries_coding(_opts.RecodeAllLocations);
        }
        
        public void StoreUnMatchedCountriesForStudies()
        {
            if (_source.source_type != "test")
            {
                org_helper.store_unmatched_country_names(source_id);
            }
        }
        
        
        public void StoreUnMatchedLocationDataForStudies()
        {
            if (_source.source_type != "test")
            {
                org_helper.store_unmatched_location_country_names(source_id);
                org_helper.store_unmatched_city_names(source_id);
            }
        }

        public void UpdateObjectIdentifiers()
        {
            org_helper.update_object_identifiers_using_names(_opts.RecodeAllOrgs);
            org_helper.update_object_identifiers_insert_default_names(_opts.RecodeAllOrgs);
        }

        public void UpdateObjectPeople()
        {
            org_helper.update_object_people_using_names(_opts.RecodeAllOrgs);
            org_helper.update_object_people_insert_default_names(_opts.RecodeAllOrgs);
        }
        
        public void UpdateObjectOrganisations()
        {
            org_helper.update_object_organisations_using_names(_opts.RecodeAllOrgs);
            org_helper.update_object_organisations_insert_default_names(_opts.RecodeAllOrgs);
        }

        public void StoreUnMatchedNamesForObjects()
        {
            if (_source.source_type != "test")
            {
                org_helper.store_unmatched_object_identifiers_org_names(source_id);
                org_helper.store_unmatched_object_organisation_org_names(source_id);
                org_helper.store_unmatched_object_people_org_names(source_id);

            }
        }

        public void UpdateStudyIEC()
        {
             // to do, will probably need complex processes....
        }


        public void UpdateDataObjectOrgs()
        {
            bool code_all = _opts.RecodeAllOrgs;
            org_helper.update_data_objects_using_names(_opts.RecodeAllOrgs);
            org_helper.update_data_objects_insert_default_names(_opts.RecodeAllOrgs);
        }
        
        public void UpdateObjectInstanceOrgs()
        {
            org_helper.update_object_instances_using_names(_opts.RecodeAllOrgs);
            org_helper.update_object_instances_insert_default_names(_opts.RecodeAllOrgs);
        }

        public void StoreUnMatchedNamesForDataObjects()
        {
            if (_source.source_type != "test")
            {
                org_helper.store_unmatched_data_object_org_names(source_id);
                org_helper.store_unmatched_object_instance_org_names(source_id);
            }
        }

        public void UpdateConditions()
        {
            if (_source.has_study_conditions is true)
            {
                topic_helper.process_conditions(_opts.RecodeAllPublishers);

                if (_source.source_type != "test")
                {
                    topic_helper.store_unmatched_condition_values(source_id);
                }
            }
        }
        
        public void UpdateTopics(string source_type)
        {
            if ((_source.source_type == "study" && _source.has_study_topics is true)
                 || source_type != "study")
            {
                topic_helper.process_topics(_opts.RecodeAllTopics);

                if (_source.source_type != "test")
                {
                    topic_helper.store_unmatched_topic_values(source_type, source_id);
                }
            }
        }

        public void DropTempTables()
        {
            org_helper.delete_temp_tables();
            topic_helper.delete_temp_tables();
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

