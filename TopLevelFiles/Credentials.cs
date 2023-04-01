using Microsoft.Extensions.Configuration;
using Npgsql;

namespace MDR_Coder;

public class Credentials : ICredentials
{
    public string Host { get; set; }
    public string Username { get; set; }
    public string Password { get; set; }
    public int Port { get; set; }

    public Credentials(IConfiguration settings)
    {
        // all asserted as non-null

        Host = settings["host"]!;
        Username = settings["user"]!;
        Password = settings["password"]!;
        string? PortAsString = settings["port"];
        if (string.IsNullOrWhiteSpace(PortAsString))
        {
            Port = 5432;
        }
        else
        {
            if (Int32.TryParse(PortAsString, out int port_num))
            {
                Port = port_num;
            }
            else
            {
                Port = 5432;
            }
        }
    }

    public string GetConnectionString(string database_name)
    {
        NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
        builder.Host = Host;
        builder.Username = Username;
        builder.Password = Password;
        builder.Port = Port;
        builder.Database = database_name;
        return builder.ConnectionString;
    }
}