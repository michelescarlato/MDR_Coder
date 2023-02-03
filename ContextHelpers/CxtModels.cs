using Npgsql;

namespace MDR_Coder
{
    public class Credentials
    {
        public string Host { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        public Credentials(string host, string user, string password)
        {
            Host = host;
            Username = user;
            Password = password;
        }

        public string GetConnectionString(string database_name, int harvest_type_id)
        {
            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
            builder.Host = Host;
            builder.Username = Username;
            builder.Password = Password;
            builder.Database = (harvest_type_id == 3) ? "test" : database_name;
            return builder.ConnectionString;
        }

    }


    public class Source
    {
        public int id { get; set; }
        public string source_type { get; set; }
        public string database_name { get; set; }
        public string db_conn { get; set; }
        public bool has_study_tables { get; set; }
        public bool has_study_topics { get; set; }
        public bool has_study_contributors { get; set; }

        public Source(int _id, string _source_type, string _database_name, string _db_conn,
                      bool _has_study_tables, bool _has_study_topics, bool _has_study_contributors)
        {
            id = _id;
            source_type = _source_type;
            database_name = _database_name;
            db_conn = _db_conn;
            has_study_tables = _has_study_tables;
            has_study_topics = _has_study_topics;
            has_study_contributors = _has_study_contributors;
        }
    }
}
