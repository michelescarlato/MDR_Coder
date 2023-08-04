using Microsoft.Extensions.Configuration;
using Dapper;
using Npgsql;

namespace MDR_Coder;

public class LoggingHelper : ILoggingHelper
{
    private readonly string? _logfileStartOfPath;
    private readonly string? _summaryLogfileStartOfPath;  
    private string _logfilePath = "";
    private string _summaryLogfilePath = "";
    private StreamWriter? _sw;

    public LoggingHelper()
    {
        IConfigurationRoot settings = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json")
            .Build();

        _logfileStartOfPath = settings["logFilePath"] ?? "";
        _summaryLogfileStartOfPath = settings["summaryFilePath"] ?? "";
    }

    public string LogFilePath => _logfilePath;

    
    public void OpenLogFile(string databaseName)
    {
        string dtString = DateTime.Now.ToString("s", System.Globalization.CultureInfo.InvariantCulture)
            .Replace(":", "").Replace("T", " ");

        string logFolderPath = Path.Combine(_logfileStartOfPath!, databaseName);
        if (!Directory.Exists(logFolderPath))
        {
            Directory.CreateDirectory(logFolderPath);
        }
        
        string logFileName = "CD " + databaseName + " " + dtString + ".log";
        _logfilePath = Path.Combine(logFolderPath, logFileName);
        _summaryLogfilePath = Path.Combine(_summaryLogfileStartOfPath!, logFileName);
        _sw = new StreamWriter(_logfilePath, true, System.Text.Encoding.UTF8);
    }
    

    public void OpenNoSourceLogFile()
    {
        string dtString = DateTime.Now.ToString("s", System.Globalization.CultureInfo.InvariantCulture)
            .Replace(":", "").Replace("T", " ");
        
        string logFileName = "CD Source not set " + dtString + ".log";
        _logfilePath = Path.Combine(_logfileStartOfPath!, logFileName);
        _summaryLogfilePath = Path.Combine(_summaryLogfileStartOfPath!, logFileName);
        _sw = new StreamWriter(_logfilePath, true, System.Text.Encoding.UTF8);
    }

    public void LogLine(string message, string identifier = "")
    {
        string dtPrefix = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string feedback = dtPrefix + message + identifier;
        Transmit(feedback);
    }
    
    public void LogBlank()
    {
        Transmit("");
    }

    public void LogHeader(string message)
    {
        string dtPrefix = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string header = dtPrefix + "**** " + message.ToUpper() + " ****";
        Transmit("");
        Transmit(header);
    }


    public void LogStudyHeader(Options opts, string dbLine)
    {
        string dividerLine = new string('=', 70);
        LogLine("");
        LogLine(dividerLine);
        LogLine(dbLine);
        LogLine(dividerLine);
        LogLine("");
    }


    public void LogError(string message)
    {
        string dtPrefix = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string errorMessage = dtPrefix + "***ERROR*** " + message;
        Transmit("");
        Transmit("+++++++++++++++++++++++++++++++++++++++");
        Transmit(errorMessage);
        Transmit("+++++++++++++++++++++++++++++++++++++++");
        Transmit("");
    }


    public void LogCodeError(string header, string errorMessage, string? stackTrace)
    {
        string dtPrefix = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string headerMessage = dtPrefix + "***ERROR*** " + header + "\n";
        Transmit("");
        Transmit("+++++++++++++++++++++++++++++++++++++++");
        Transmit(headerMessage);
        Transmit(errorMessage + "\n");
        Transmit(stackTrace ?? "");
        Transmit("+++++++++++++++++++++++++++++++++++++++");
        Transmit("");
    }


    public void LogParseError(string header, string errorNum, string errorType)
    {
        string dtPrefix = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string errorMessage = dtPrefix + "***ERROR*** " + "Error " + errorNum + ": " + header + " " + errorType;
        Transmit(errorMessage);
    }
            

    public void LogCommandLineParameters(Options opts)
    {
        int[]? sourceIds = opts.SourceIds?.ToArray();
        if (sourceIds?.Length > 0)
        {
            if (sourceIds.Length == 1)
            {
                LogLine("Source_id is " + sourceIds[0]);
            }
            else
            {
                LogLine("Source_ids are " + string.Join(",", sourceIds));
            }
        }

        if (opts.RecodeOrgs > 0)
        {
            if (opts.RecodeOrgs == 1)
            {
                LogLine("Recoding unmatched organisation data");
            }
            else if (opts.RecodeOrgs == 2)
            {
                LogLine("Recoding all organisation data, not just recently added");
            }
        }
        if (opts.RecodeLocations > 0)
        {
            if (opts.RecodeLocations == 1)
            {
                LogLine("Recoding unmatched country / location data");
            }
            else if (opts.RecodeLocations == 2)
            {
                LogLine("Recoding all country / location data, not just recently added");
            }
        }
        if (opts.RecodeTopics > 0)
        {
            if (opts.RecodeTopics == 1)
            {
                LogLine("Recoding unmatched topic data");
            }
            else if (opts.RecodeTopics == 2)
            {
                LogLine("Recoding all topic data, not just recently added");
            }
        }
        if (opts.RecodeConditions > 0)
        {
            if (opts.RecodeConditions == 1)
            {
                LogLine("Recoding unmatched condition data");
            }
            else if (opts.RecodeConditions == 2)
            {
                LogLine("Recoding all condition data, not just recently added");
            }
        }
        if (opts.RecodePublishers > 0)
        {
            if (opts.RecodePublishers == 1)
            {
                LogLine("Recoding unmatched publisher data");
            }
            else if (opts.RecodePublishers == 2)
            {
                LogLine("Recoding all publisher data, not just recently added");
            }
        }
        LogBlank();
    }


    public void CloseLog()
    {
        if (_sw is not null)
        {
            LogHeader("Closing Log");
            _sw.Flush();
            _sw.Close();
        }
        
        // Write out the summary file.
        
        //var swSummary = new StreamWriter(_summaryLogfilePath, true, System.Text.Encoding.UTF8);
        
        //swSummary.Flush();
        //swSummary.Close();
    }
    
    
    private void Transmit(string message)
    {
        _sw!.WriteLine(message);
        Console.WriteLine(message);
    }


    public string GetTableRecordCount(string dbConn, string schema, string tableName)
    {
        string sqlString = "select count(*) from " + schema + "." + tableName;

        using NpgsqlConnection conn = new NpgsqlConnection(dbConn);
        int res = conn.ExecuteScalar<int>(sqlString);
        return res.ToString() + " records found in " + schema + "." + tableName;
    }
  
}