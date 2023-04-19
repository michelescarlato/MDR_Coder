using Dapper;
using Npgsql;

namespace MDR_Coder
{
    public class CodingBuilder
    {
        private readonly string connString;
        private readonly Source _source;
        private readonly Options _opts;
        private readonly ILoggingHelper _logging_helper;
        private readonly int source_id;
        
        private readonly OrgHelper org_helper;
        private readonly PubmedHelper pubmed_helper;
        private readonly TopicHelper topic_helper;
        private readonly ConditionHelper condition_helper;
        private CodeEvent? codeEvent = null;

        public CodingBuilder(Source source, Options opts, ILoggingHelper logging_helper)
        {
            _source = source;
            connString = source.db_conn ?? "";
            source_id = source.id ?? 0;
            
            _opts = opts;
            _logging_helper = logging_helper;
            
            pubmed_helper = new PubmedHelper(source, logging_helper);
            org_helper = new OrgHelper(source, logging_helper);
            topic_helper = new TopicHelper(source, logging_helper);
            condition_helper = new ConditionHelper(source, logging_helper);
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

        public CodeEvent CreateCodingEvent(int codingId)
        {
            CodeEvent coding = new CodeEvent(codingId, _source.id);
            codeEvent = coding;
            return coding;
        }
        
        public void EstablishTempTables()
        {
            org_helper.establish_temp_tables();
        }

        public void ObtainPublisherInformation()
        {
            pubmed_helper.clear_publisher_names(_opts.RecodeAllPublishers);
            pubmed_helper.obtain_publisher_names(_opts.RecodeAllPublishers);
        }


        public void ApplyPublisherData()
        {
            pubmed_helper.update_objects_publisher_data(_opts.RecodeAllPublishers);
            pubmed_helper.update_identifiers_publisher_data(_opts.RecodeAllPublishers);
            if (_source.source_type != "test")
            {
                codeEvent!.num_publishers_to_match += pubmed_helper.store_unmatched_publisher_org_names(source_id);
            }
        }


        public void UpdateStudyIdentifiers()
        {
            org_helper.update_study_identifiers(_opts.RecodeAllOrgs);
            if (_source.source_type != "test")
            {
                codeEvent!.num_orgs_to_match += org_helper.store_unmatched_study_identifiers_org_names(source_id);
            }
        }

        public void UpdateStudyOrgs()
        {
            org_helper.update_study_organisations(_opts.RecodeAllOrgs);
            org_helper.update_missing_sponsor_ids();
            if (_source.source_type != "test")
            {
                codeEvent!.num_orgs_to_match += org_helper.store_unmatched_study_organisation_names(source_id);
            }

        }

        public void UpdateStudyPeople()
        {
            org_helper.update_study_people(_opts.RecodeAllOrgs);
            if (_source.source_type != "test")
            {
                codeEvent!.num_orgs_to_match += org_helper.store_unmatched_study_people_org_names(source_id);
            }
        }

        public void UpdateStudyCountries()
        {
            org_helper.update_study_countries(_opts.RecodeAllLocations);
            if (_source.source_type != "test")
            {
                codeEvent!.num_countries_to_match += org_helper.store_unmatched_country_names(source_id);
            }
        }
        
        public void UpdateStudyLocations()
        {
            org_helper.update_studylocation_orgs(_opts.RecodeAllLocations);            
            //org_helper.update_studylocation_countries(_opts.RecodeAllLocations);
            if (_source.source_type != "test")
            {
                codeEvent!.num_countries_to_match += org_helper.store_unmatched_location_country_names(source_id);
                codeEvent!.num_cities_to_match += org_helper.store_unmatched_city_names(source_id);
            }
        }

        public void UpdateStudyIEC()
        {
             // to do, will probably need complex processes....
        }


        public void UpdateDataObjectOrgs()
        {
            org_helper.update_data_objects(_opts.RecodeAllOrgs);
            if (_source.source_type != "test")
            {
                codeEvent!.num_orgs_to_match += org_helper.store_unmatched_data_object_org_names(source_id);
            }
        }
        
        public void UpdateObjectInstanceOrgs()
        {
            org_helper.update_object_instances(_opts.RecodeAllOrgs);
            if (_source.source_type != "test")
            {
                codeEvent!.num_orgs_to_match += org_helper.store_unmatched_object_instance_org_names(source_id);
            }
        }
        
        
        public void UpdateObjectIdentifiers()
        {
            org_helper.update_object_identifiers(_opts.RecodeAllOrgs);
            if (_source.source_type != "test")
            {
                codeEvent!.num_orgs_to_match += org_helper.store_unmatched_object_identifiers_org_names(source_id);
            }
        }

        public void UpdateObjectPeople()
        {
            org_helper.update_object_people(_opts.RecodeAllOrgs);
            if (_source.source_type != "test")
            {
                codeEvent!.num_orgs_to_match += org_helper.store_unmatched_object_people_org_names(source_id);
            }
        }
        
        public void UpdateObjectOrganisations()
        {
            org_helper.update_object_organisations(_opts.RecodeAllOrgs);
            if (_source.source_type != "test")
            {
                codeEvent!.num_orgs_to_match += org_helper.store_unmatched_object_organisation_org_names(source_id);
            }
        }
       
        
        public void UpdateTopics(string source_type)
        {
            if (_source is { source_type: "study", has_study_topics: true }
                 || source_type != "study")
            {
                topic_helper.process_topics(_opts.RecodeAllTopics);

                if (_source.source_type != "test")
                {
                    codeEvent!.num_topics_to_match += topic_helper.store_unmatched_topic_values(source_type, source_id);
                }
            }
        }
        
        public void UpdateConditions()
        {
            if (_source.has_study_conditions is true)
            {
                condition_helper.process_conditions(_opts.RecodeAllConditions);

                if (_source.source_type != "test")
                {
                    codeEvent!.num_conditions_to_match += condition_helper.store_unmatched_condition_values(source_id);
                }
            }
        }

        public void DropTempTables()
        {
            org_helper.delete_temp_tables();
            topic_helper.delete_temp_tables();
            condition_helper.delete_temp_tables();
        }
        
        
        public void UpdateStudiesImportedDateInMon(int codingId)
        {
            string top_string = @"Update mn.source_data src
                      set last_coding_id = " + codingId + @", 
                      last_coded = current_timestamp
                      from 
                         (select so.sd_sid 
                         FROM sd.studies so ";
            string base_string = @" ) s
                          where s.sd_sid = src.sd_sid;";

            UpdateLastImportedDate("studies", top_string, base_string);
        }

    
        public void UpdateObjectsImportedDateInMon(int codingId)
        {
            string top_string = @"UPDATE mn.source_data src
                      set last_coding_id = " + codingId + @", 
                      last_coded = current_timestamp
                      from 
                         (select so.sd_oid 
                          FROM sd.data_objects so ";
            string base_string = @" ) s
                          where s.sd_oid = src.sd_oid;";

            UpdateLastImportedDate("data_objects", top_string, base_string);
        }

        private void UpdateLastImportedDate(string tableName, string topSql, string baseSql)
        {
            try
            {   
                using NpgsqlConnection conn = new(connString);
                string feedbackA = "Updating monitor records with date time of coding, ";
                string sqlString = $"select count(*) from sd.{tableName}";
                int recCount  = conn.ExecuteScalar<int>(sqlString);
                int recBatch = 50000;
                if (recCount > recBatch)
                {
                    for (int r = 1; r <= recCount; r += recBatch)
                    {
                        sqlString = topSql + 
                                    " where so.id >= " + r + " and so.id < " + (r + recBatch)
                                    + baseSql;
                        conn.Execute(sqlString);
                        string feedback = feedbackA + r + " to ";
                        feedback += (r + recBatch < recCount) ? (r + recBatch - 1).ToString() : recCount.ToString();
                        _logging_helper.LogLine(feedback);
                    }
                }
                else
                {
                    sqlString = topSql + baseSql;
                    conn.Execute(sqlString);
                    _logging_helper.LogLine(feedbackA + recCount + " as a single query");
                }
            }
            catch (Exception e)
            {
                string res = e.Message;
                _logging_helper.LogError("In update last imported date (" + tableName + "): " + res);
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

