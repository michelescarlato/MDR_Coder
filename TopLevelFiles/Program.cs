using System.Reflection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using MDR_Coder;

// Set up file based configuration environment.

string assemblyLocation = Assembly.GetExecutingAssembly().Location;
string? basePath = Path.GetDirectoryName(assemblyLocation);
if (string.IsNullOrWhiteSpace(basePath))
{
    return -1;
}

var configFiles = new ConfigurationBuilder()
    .SetBasePath(basePath)
    .AddJsonFile("appsettings.json")
    .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")}.json", true)
    .Build();

// Set up the host for the app,
// adding the services used in the system to support DI

IHost host = Host.CreateDefaultBuilder()
    .UseContentRoot(basePath)
    .ConfigureAppConfiguration(builder =>
    {
        builder.AddConfiguration(configFiles); 
        
    })
    .ConfigureServices(services =>
    {
        services.AddSingleton<ICredentials, Credentials>();
        services.AddSingleton<ILoggingHelper, LoggingHelper>();
        services.AddSingleton<IMonDataLayer, MonDataLayer>();        
    })
    .Build();

// Establish logger, at this stage as an object reference
// because the log file(s) are yet to be opened.
// Establish a new credentials class, and use both to establish the monitor 
// data (repository) layer. ALL of these classes are singletons.

LoggingHelper loggingHelper = ActivatorUtilities.CreateInstance<LoggingHelper>(host.Services);
Credentials credentials = ActivatorUtilities.CreateInstance<Credentials>(host.Services);
MonDataLayer monDataLayer = new(credentials, loggingHelper);

// Establish the parameter checker, which first checks if the program's 
// arguments can be parsed and, if they can, then checks if they are valid.
// If both tests are passed the object returned includes both the
// original arguments and the data source or sources (database ad schemas) being coded.

ParameterChecker paramChecker = new(monDataLayer, loggingHelper);
ParamsCheckResult paramsCheck = paramChecker.CheckParams(args);
if (paramsCheck.ParseError || paramsCheck.ValidityError)
{
    // End program, parameter errors should have been logged
    // in a 'no source' file by the ParameterChecker class.

    return -1;
}

// Should be able to proceed - opts and source(s) are known to be non-null.
// For a normal run, create an Importer class and run the import process.
// For a test run, create a test importer, which uses exactly the same code
// but which needs to establish a framework for the test data first, and then
// compare it with expected data afterwards.

try
{
    var opts = paramsCheck.Pars!;
    Coder coder = new(monDataLayer, loggingHelper);
    coder.Run(opts);
    return 0;
}

catch (Exception e)
{
    // If an error bubbles up to here there is an issue with the code.
    // Any logger at this stage will be connected to a sink file.

    loggingHelper.LogHeader("UNHANDLED EXCEPTION");
    loggingHelper.LogCodeError("MDR_Coder application aborted", e.Message, e.StackTrace);
    loggingHelper.CloseLog();
    return -1;
}


