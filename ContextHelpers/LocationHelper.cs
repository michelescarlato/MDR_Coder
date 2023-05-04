using Dapper;
using Npgsql;

namespace MDR_Coder
{
    public class LocHelper
    {
        private readonly string _db_conn;
        private readonly string _schema;
        private readonly ILoggingHelper _loggingHelper;     

        public LocHelper(Source source, ILoggingHelper logger)
        {
            _db_conn = source.db_conn ?? "";
            _schema = "ad"; 
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
        
        private int GetMinId(string schema, string table_name)
        {
            string sql_string = @$"select min(id) from {schema}.{table_name};";
            using var conn = new NpgsqlConnection(_db_conn);
            return conn.ExecuteScalar<int>(sql_string);
        }
        
        private int GetMaxId(string schema, string table_name)
        {
            string sql_string = @$"select max(id) from {schema}.{table_name};";
            using var conn = new NpgsqlConnection(_db_conn);
            return conn.ExecuteScalar<int>(sql_string);
        }
        
        private void FeedbackCountryResults(string schema, string table_name)
        {
            int table_count = GetTableCount(schema, table_name);
            int coded_count = GetFieldCount(schema, table_name, "country_id");
            _loggingHelper.LogLine($"{coded_count} records, from {table_count}, " +
                                   $"{(double) 100 * coded_count / table_count:N1}%, " +  
                                   $"have coded countries in {schema}.{table_name}");
        }
        
        private void FeedbackLocationResults(string schema, string table_name)
        {
            int table_count = GetTableCount(schema, table_name);
            int coded_count = GetFieldCount(schema, table_name, "facility_org_id");
            _loggingHelper.LogLine($"{coded_count} records, from {table_count}, " +
                                   $"{(double) 100 * coded_count / table_count:N1} %, " + 
                                   $"have coded facilities in {schema}.{table_name}");
            coded_count = GetFieldCount(schema, table_name, "city_id");
            _loggingHelper.LogLine($"{coded_count} records, from {table_count}, " +
                                   $"{(double) 100 * coded_count / table_count:N1} %, " + 
                                   $"have coded cities in {schema}.{table_name}");
            coded_count = GetFieldCount(schema, table_name, "country_id");
            _loggingHelper.LogLine($"{coded_count} records, from {table_count}, " +
                                   $"{(double) 100 * coded_count / table_count:N1} %, " +  
                                   $"have coded countries in {schema}.{table_name}");
        }
        
        // Set up relevant names for comparison
        
        public void establish_temp_tables()
        {
            string sql_string = $@"drop table if exists {_schema}.temp_country_names;
                 create table {_schema}.temp_country_names 
                 as 
                 select a.geoname_id, country_name, lower(a.alt_name) as name from 
                 context_ctx.country_names a";
            Execute_SQL(sql_string);
            
            sql_string = $@"drop table if exists {_schema}.temp_city_names;
                 create table {_schema}.temp_city_names 
                 as 
                 select a.geoname_id, city_name, 
                    lower(a.alt_name) as name,
                    lower(a.country_name) as country_name
                 from 
                 context_ctx.city_names a";
            Execute_SQL(sql_string);
        }
        
        // Code country names
        
        public void update_study_countries(bool code_all)
        {
            int min_id = GetMinId(_schema, "study_countries");
            int max_id = GetMaxId(_schema, "study_countries");
            string qualifier = code_all ? "all" : "unmatched"; 
            
            string sql_string = $@"update {_schema}.study_countries c
            set country_id = n.geoname_id
            from {_schema}.temp_country_names n
            where c.country_name is not null
            and lower(c.country_name) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";

            string action = $"Coding {qualifier} country names";
            Execute_LocationSQL(min_id, max_id, 200000, sql_string, action);
            
            sql_string = $@"update {_schema}.study_countries c
                        set country_name = n.country_name, 
                        coded_on = CURRENT_TIMESTAMP
                        from {_schema}.temp_country_names n 
                        where c.country_id = n.geoname_id ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            action = $"Inserting default country names for {qualifier} countries";
            Execute_LocationSQL(min_id, max_id, 200000, sql_string, action);

            FeedbackCountryResults("ad", "study_countries");
        }
           
        
        // Code location city and country codes
        
        public void update_studylocation_orgs(bool code_all)
        {
            int min_id = GetMinId(_schema, "study_locations");
            int max_id = GetMaxId(_schema, "study_locations");
            string qualifier = code_all ? "all" : "unmatched"; 
            
            RemoveInitialThes(_schema + ".study_locations", "facility", min_id, max_id, 200000);
            
            string sql_string = $@"update {_schema}.study_locations c
            set facility_org_id = n.org_id
            from {_schema}.temp_org_names n
            where c.facility is not null
            and lower(c.facility) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            string action = $"Coding {qualifier} facility names";
            Execute_LocationSQL(min_id, max_id, 100000, sql_string, action);
            
            sql_string = $@"update {_schema}.study_locations c
            set facility = g.default_name,
            facility_ror_id = g.ror_id
            from context_ctx.organisations g
            where c.facility_org_id = g.id ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            action = $"Inserting default facility names for {qualifier} facilities";
            Execute_LocationSQL(min_id, max_id, 100000, sql_string, action);
            
            sql_string = $@"update {_schema}.study_locations c
            set city_id = locs.city_id,
            city_name = locs.city,
            country_id = locs.country_id,
            country_name = locs.country,
            coded_on = CURRENT_TIMESTAMP
            from context_ctx.org_locations locs
            where c.facility_org_id = locs.org_id ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            action = $"Added location city and country default names for {qualifier} facilities";
            Execute_LocationSQL(min_id, max_id, 100000, sql_string, action);
            
            // Now considering cities that have not been coded. try to code them
            // An element of ambiguity here, so use the country as well.
            // Still not perfect but better than nothing...
            
            sql_string = $@"update {_schema}.study_locations c
            set city_id = n.geoname_id
            from {_schema}.temp_city_names n
            where c.city_name is not null
            and lower(c.city_name) = n.name 
            and lower(c.country_name) = n.country_name ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            action = $"Coding {qualifier} city names, not coded using facility data";
            Execute_LocationSQL(min_id, max_id, 100000, sql_string, action);

            sql_string = $@"update {_schema}.study_locations c
                        set city_name = n.city_name
                        from {_schema}.temp_city_names n 
                        where c.city_id = n.geoname_id ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            action = $"Inserting default names for {qualifier} cities not coded using facility data";
            Execute_LocationSQL(min_id, max_id, 100000, sql_string, action);
            
            // Finally consider countries that have not been coded.

            sql_string = $@"update {_schema}.study_locations c
                set country_id = n.geoname_id
                from {_schema}.temp_country_names n
                where c.country_name is not null
                and lower(c.country_name) = n.name ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            action = $"Coding {qualifier} country names, not coded using facility data";
            Execute_LocationSQL(min_id, max_id, 100000, sql_string, action);
            
            sql_string = $@"update {_schema}.study_locations c
                        set country_name = n.country_name,
                        coded_on = CURRENT_TIMESTAMP
                        from {_schema}.temp_country_names n 
                        where c.country_id = n.geoname_id ";
            sql_string += code_all ? "" : " and coded_on is null ";
            
            action = $"Inserting default names for {qualifier} countries not coded using facility data";
            Execute_LocationSQL(min_id, max_id, 100000, sql_string, action);
            FeedbackLocationResults("ad", "study_locations");
        }

        
        private void Execute_LocationSQL(int min_id, int max_id, int rec_batch, string base_sql, string action )
        {
            try
            {
                if (max_id - min_id > rec_batch)
                {
                    for (int r = min_id; r <= max_id; r += rec_batch)
                    {
                        string batch_sql_string = base_sql + " and c.id >= " + r + " and c.id < " + (r + rec_batch);
                        int res1 = Execute_SQL(batch_sql_string);
                        int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                        string feedback = $"{action} - {res1} in records {r} to {e}";
                        _loggingHelper.LogLine(feedback);
                    }
                }
                else
                {
                    int res = Execute_SQL(base_sql);
                    _loggingHelper.LogLine($"{action} - {res} done as a single query");
                }
            }
            catch (Exception e)
            {
                string eres = e.Message;
                _loggingHelper.LogError($"In {action}: " + eres);
            }
        }
        

        private void RemoveInitialThes(string table_name, string field_name, int min_id, int max_id, int rec_batch)
        {
            string action = $"Removing initial 'The's from org names in {table_name}";
            
            string base_sql = $@"update {table_name} c
            set {field_name} = trim(substring({field_name}, 4)) 
            where {field_name} ilike 'The %'
            and cardinality(string_to_array({field_name} , ' ')) > 2";
            
            try
            {
                if (max_id - min_id > rec_batch)
                {
                    for (int r = 1; r <= max_id; r += rec_batch)
                    {
                        string batch_sql_string = base_sql + " and c.id >= " + r + " and c.id < " + (r + rec_batch);
                        int res1 = Execute_SQL(batch_sql_string);
                        int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                        string feedback = $"{action} - {res1} records in ids {r} to {e}";
                        _loggingHelper.LogLine(feedback);
                    }
                }
                else
                {
                    int res = Execute_SQL(base_sql);
                    _loggingHelper.LogLine($"{action} - {res} records done as a single query");
                }
            }
            catch (Exception e)
            {
                string eres = e.Message;
                _loggingHelper.LogError($"In {action}: " + eres);
            }
        }
        
        private int StoreUncodedCities(int rec_batch, int source_id)
        {
            string action = $"Storing uncoded city names from study_locations";
            int min_id = GetMinId(_schema, "study_locations");
            int max_id = GetMaxId(_schema, "study_locations");
            int number_of_loops = 0;
            string base_sql = $@"insert into context_ctx.to_match_cities 
                                 (source_id, source_table, city_name, number_of) 
                                 select {source_id}, 'study_locations', city_name, count(city_name) 
                                 from {_schema}.study_locations c
                                 where city_id is null ";
            try
            {
                if (max_id - min_id > rec_batch)
                {
                    for (int r = 1; r <= max_id; r += rec_batch)
                    {
                        string batch_sql_string = base_sql + " and c.id >= " + r + " and c.id < " + (r + rec_batch);
                        batch_sql_string += " group by city_name;";
                        int res1 = Execute_SQL(batch_sql_string);
                        int e = r + rec_batch < max_id ? r + rec_batch : max_id;
                        string feedback = $"{action} - {res1} records in ids {r} to {e}";
                        _loggingHelper.LogLine(feedback);
                        number_of_loops++;
                    }
                }
                else
                {
                    base_sql += " group by city_name;";
                    int res = Execute_SQL(base_sql);
                    _loggingHelper.LogLine($"{action} - {res} records done as a single query");
                    number_of_loops = 1;
                }

                return number_of_loops;
            }
            catch (Exception e)
            {
                string eres = e.Message;
                _loggingHelper.LogError($"In {action}: " + eres);
                return 0;
            }
        }
        
        private void AggregateUncodedCityData(int source_id)
        {
            // Create a temp table with the aggregated data, delete the 
            // existing data, and replace with the aggregated set
            
            string sql_string = $@"create table {_schema}.temp_city_data as
                            select source_id, city_name, 
                            sum(number_of) as number_of
                            from context_ctx.to_match_cities 
                            where source_id = {source_id}
                            group by source_id, city_name; ";
           Execute_SQL(sql_string);    // number of aggregated records
           
           sql_string = $@"delete from context_ctx.to_match_cities 
                           where source_id = {source_id} 
                           and source_table = 'study_locations';";
           Execute_SQL(sql_string);
           
           sql_string = $@"insert into context_ctx.to_match_cities 
                           (source_id, source_table, city_name, number_of) 
                           select source_id, 'study_locations', city_name, number_of
                           from {_schema}.temp_city_data ;";
           Execute_SQL(sql_string);

           sql_string = $@"drop table {_schema}.temp_city_data ";
           Execute_SQL(sql_string);
        }

        
        // Store unmatched names
       
        public int store_unmatched_country_names(int source_id)
        {
            string sql_string = $@"delete from context_ctx.to_match_countries 
                                   where source_id = {source_id} 
                                   and source_table = 'study_countries';";
            Execute_SQL(sql_string); 
            sql_string = $@"insert into context_ctx.to_match_countries (source_id, source_table, country_name, number_of) 
            select {source_id}, 'study_countries', country_name, count(country_name) 
            from {_schema}.study_countries 
            where country_id is null 
            group by country_name; ";

            int res = Execute_SQL(sql_string); 
            _loggingHelper.LogLine($"Stored {res} unmatched country names, for review");
            return res;
        }
        
        
        public int store_unmatched_location_country_names(int source_id)
        {
            string sql_string = $@"delete from context_ctx.to_match_countries 
                                   where source_id = {source_id} 
                                   and source_table = 'study_locations';";
            Execute_SQL(sql_string); 
            sql_string = $@"insert into context_ctx.to_match_countries (source_id, source_table, country_name, number_of) 
            select {source_id}, 'study_locations', country_name, count(country_name) 
            from {_schema}.study_locations 
            where country_id is null 
            group by country_name; ";

            int res = Execute_SQL(sql_string); 
            _loggingHelper.LogLine($"Stored {res} unmatched location country names, for review");
            return res;
        }
        
        
        public int store_unmatched_city_names(int source_id)
        {
            string sql_string = $@"delete from context_ctx.to_match_cities 
                                  where source_id = {source_id} 
                                  and source_table = 'study_locations';";
            Execute_SQL(sql_string);

            // Store in batches because locations table may be very large,
            // and then aggregate within the no-match table.
            
            if (StoreUncodedCities(100000, source_id) > 1)
            {
                AggregateUncodedCityData(source_id);
            }
             
            // Get total records that were not matched.
            
            sql_string = $@"select count(*) from context_ctx.to_match_cities 
                            where source_id = {source_id};";
            using var conn = new NpgsqlConnection(_db_conn);
            int res = conn.ExecuteScalar<int>(sql_string);

            _loggingHelper.LogLine($"Stored {res} unmatched location city names, for review");
            return res;
        }
        
        public void delete_temp_tables()
        {
            string sql_string = $@"drop table {_schema}.temp_country_names;
                                   drop table {_schema}.temp_city_names ";

            Execute_SQL(sql_string);
        }
    }
}
