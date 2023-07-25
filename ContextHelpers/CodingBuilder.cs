using Dapper;
using Npgsql;

namespace MDR_Coder
{
    public class CodingBuilder
    {
        private readonly string connString;
        private readonly Source _source;
        private readonly ILoggingHelper _logging_helper;
        private readonly int source_id;
        
        private readonly OrgHelper org_helper;
        private readonly PubmedHelper pubmed_helper;
        private readonly LocHelper location_helper;
        private readonly TopicHelper topic_helper;
        private readonly ConditionHelper condition_helper;

        private CodeEvent? codeEvent;

        public CodingBuilder(Source source, Options opts, ILoggingHelper logging_helper)
        {
            _source = source;
            connString = source.db_conn ?? "";
            source_id = source.id ?? 0;
            _logging_helper = logging_helper;
            bool testDataOnly = opts.ReCodeTestDataOnly == true;
            
            pubmed_helper = new PubmedHelper(source, logging_helper, (int)opts.RecodePublishers!, testDataOnly) ;
            org_helper = new OrgHelper(source, logging_helper, (int)opts.RecodeOrgs!, testDataOnly);
            location_helper = new LocHelper(source, logging_helper, (int)opts.RecodeLocations!, testDataOnly);
            topic_helper = new TopicHelper(source, logging_helper, (int)opts.RecodeTopics!, testDataOnly);
            condition_helper = new ConditionHelper(source, logging_helper, (int)opts.RecodeConditions!, testDataOnly);
        }


        public void EstablishContextForeignTables(Credentials creds)
        {
            using var conn = new NpgsqlConnection(connString);
            
            string sql_string = @"CREATE EXTENSION IF NOT EXISTS postgres_fdw
                                     schema sd";   // any schema will do
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

        public CodeEvent CreateCodingEvent(int codingId)
        {
            CodeEvent coding = new CodeEvent(codingId, _source.id);
            codeEvent = coding;
            return coding;
        }
        
        public void EstablishTempTables()
        {
            org_helper.establish_temp_tables();
            location_helper.establish_temp_tables();
        }

        public void ObtainPublisherInformation()
        {
            pubmed_helper.clear_publisher_names();
            pubmed_helper.obtain_publisher_names();
            _logging_helper.LogLine("");
        }


        public void ApplyPublisherData()
        {
            pubmed_helper.update_objects_publisher_data();
            pubmed_helper.update_identifiers_publisher_data();
            codeEvent!.num_publishers_to_match += pubmed_helper.store_unmatched_publisher_org_names(source_id);
            _logging_helper.LogBlank();
        }
        
        public void UpdateStudyIdentifiers()
        {
            org_helper.update_study_identifiers();
            codeEvent!.num_orgs_to_match += org_helper.store_unmatched_study_identifiers_org_names(source_id);
            _logging_helper.LogBlank();
        }
        
        public void UpdateStudyOrgs()
        {
            org_helper.update_study_organisations();
            codeEvent!.num_orgs_to_match += org_helper.store_unmatched_study_organisation_names(source_id);
            _logging_helper.LogBlank();
        }

        public void UpdateStudyPeople()
        {
            org_helper.update_study_people();
            codeEvent!.num_orgs_to_match += org_helper.store_unmatched_study_people_org_names(source_id);
            _logging_helper.LogBlank();
        }
        
        public void UpdateDataObjectOrgs()
        {
            org_helper.update_data_objects();
            codeEvent!.num_orgs_to_match += org_helper.store_unmatched_data_object_org_names(source_id);
            _logging_helper.LogBlank();
        }
        
        public void UpdateObjectInstanceOrgs()
        {
            org_helper.update_object_instances();
            codeEvent!.num_orgs_to_match += org_helper.store_unmatched_object_instance_org_names(source_id);
            _logging_helper.LogBlank();
        }
        
        
        public void UpdateObjectIdentifiers()
        {
            org_helper.update_object_identifiers();
            codeEvent!.num_orgs_to_match += org_helper.store_unmatched_object_identifiers_org_names(source_id);
            _logging_helper.LogBlank();
        }

        public void UpdateObjectPeople()
        {
            org_helper.update_object_people();
            codeEvent!.num_orgs_to_match += org_helper.store_unmatched_object_people_org_names(source_id);
            _logging_helper.LogBlank();
        }
        
        public void UpdateObjectOrganisations()
        {
            org_helper.update_object_organisations();
            codeEvent!.num_orgs_to_match += org_helper.store_unmatched_object_organisation_org_names(source_id);
            _logging_helper.LogBlank();
        }
        
        public void DropTempOrgTables()
        {
            org_helper.delete_temp_tables();
        }  
        
        public void UpdateStudyCountries()
        {
            if (_source.has_study_countries is true)
            {
                location_helper.update_study_countries();
                codeEvent!.num_countries_to_match += location_helper.store_unmatched_country_names(source_id);
                _logging_helper.LogBlank();
            }
        }
        
        public void UpdateStudyLocations()
        {
            if (_source.has_study_locations is true)
            {
                location_helper.update_studylocation_orgs();
                codeEvent!.num_countries_to_match += location_helper.store_unmatched_location_country_names(source_id);
                codeEvent!.num_cities_to_match += location_helper.store_unmatched_city_names(source_id);
                _logging_helper.LogBlank();
                location_helper.delete_temp_tables();
            }
        }
        
        public void UpdateTopics(string source_type)
        {
            if (_source is { source_type: "study", has_study_topics: true }
                 || source_type == "object")
            {
                topic_helper.process_topics();
                codeEvent!.num_topics_to_match += topic_helper.store_unmatched_topic_values(source_id);
                _logging_helper.LogBlank();
            }
        }
        
        public void UpdateConditions()
        {
            if (_source.has_study_conditions is true)
            {
                condition_helper.process_conditions();
                codeEvent!.num_conditions_to_match += condition_helper.store_unmatched_condition_values(source_id);
                _logging_helper.LogBlank();              
            }
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

