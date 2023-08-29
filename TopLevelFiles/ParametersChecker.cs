using CommandLine;
namespace MDR_Coder;

internal class ParameterChecker 
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly IMonDataLayer _monDataLayer;

    public ParameterChecker(IMonDataLayer monDataLayer, ILoggingHelper loggingHelper)
    {
        _monDataLayer = monDataLayer;
        _loggingHelper = loggingHelper;
    }
    
    public ParamsCheckResult CheckParams(string[]? args)
    {
        // Calls the CommandLine parser. If an error in the initial parsing, log it 
        // and return an error. If parameters can be passed, check their validity
        // and if invalid log the issue and return an error, otherwise return the 
        // parameters, processed as an instance of the Options class, and the source.

        var parsedArguments = Parser.Default.ParseArguments<Options>(args);
        if (parsedArguments.Errors.Any())
        {
            LogParseError(((NotParsed<Options>)parsedArguments).Errors);
            return new ParamsCheckResult(true, false, null);
        }

        var opts = parsedArguments.Value;
        return CheckArgumentValuesAreValid(opts);

    }
    
   
    public ParamsCheckResult CheckArgumentValuesAreValid(Options opts)
    {
        // 'opts' is passed by reference and may be changed by the checking mechanism.

        try
        {
            if (opts.SourceIds?.Any() is not true)
            {
                throw new ArgumentException("No source id provided");
            }

            foreach (int sourceId in opts.SourceIds)
            {
                if (!_monDataLayer.SourceIdPresent(sourceId))
                {
                    throw new ArgumentException("Source argument " + sourceId.ToString() +
                                                " does not correspond to a known source");
                }
            }

            opts.RecodeAll ??= 0;
            opts.RecodeOrgs ??= 0;
            opts.RecodeConditions ??= 0;
            opts.RecodeTopics ??= 0;
            opts.RecodeLocations ??= 0;
            opts.RecodePublishers ??= 0;
            
            if (opts.RecodeAll == 1)   
            {
                opts.RecodeOrgs = 1;
                opts.RecodeConditions = 1;
                opts.RecodeTopics = 1;
                opts.RecodeLocations = 1;
                opts.RecodePublishers = 1;
            }
            
            if (opts.ReCodeTestDataOnly)
            {
                opts.RecodeAll = 2;
            }
            
            if (opts.RecodeAll == 2)    
            {
                opts.RecodeOrgs = 2;
                opts.RecodeConditions = 2;
                opts.RecodeTopics = 2;
                opts.RecodeLocations = 2;
                opts.RecodePublishers = 2;
            }
            
            if (opts.RecodeOrgs > 2) opts.RecodeOrgs = 2;
            if (opts.RecodeConditions > 2) opts.RecodeConditions = 2;
            if (opts.RecodeTopics > 2) opts.RecodeTopics = 2;
            if (opts.RecodeLocations > 2) opts.RecodeLocations = 2;
            if (opts.RecodePublishers > 2) opts.RecodePublishers = 2;
            
            // parameters valid - return opts, now with valid non null values

            return new ParamsCheckResult(false, false, opts);
        }

        catch (Exception e)
        {
            _loggingHelper.OpenNoSourceLogFile();
            _loggingHelper.LogHeader("INVALID PARAMETERS");
            _loggingHelper.LogCommandLineParameters(opts);
            _loggingHelper.LogCodeError("Coder application aborted", e.Message, e.StackTrace);
            _loggingHelper.CloseLog();
            return new ParamsCheckResult(false, true, null);
        }
    }


    internal void LogParseError(IEnumerable<Error> errs)
    {
        _loggingHelper.OpenNoSourceLogFile();
        _loggingHelper.LogHeader("UNABLE TO PARSE PARAMETERS");
        _loggingHelper.LogHeader("Error in input parameters");
        _loggingHelper.LogLine("Error in the command line arguments - they could not be parsed");

        int n = 0;
        foreach (Error e in errs)
        {
            n++;
            _loggingHelper.LogParseError("Error {n}: Tag was {Tag}", n.ToString(), e.Tag.ToString());
            if (e.GetType().Name == "UnknownOptionError")
            {
                _loggingHelper.LogParseError("Error {n}: Unknown option was {UnknownOption}", n.ToString(), ((UnknownOptionError)e).Token);
            }
            if (e.GetType().Name == "MissingRequiredOptionError")
            {
                _loggingHelper.LogParseError("Error {n}: Missing option was {MissingOption}", n.ToString(), ((MissingRequiredOptionError)e).NameInfo.NameText);
            }
            if (e.GetType().Name == "BadFormatConversionError")
            {
                _loggingHelper.LogParseError("Error {n}: Wrongly formatted option was {MissingOption}", n.ToString(), ((BadFormatConversionError)e).NameInfo.NameText);
            }
        }
        _loggingHelper.LogLine("MDR_Downloader application aborted");
        _loggingHelper.CloseLog();
    }

}

public class Options
{
    // Lists the command line arguments and options

    [Option('s', "source_ids", Required = false, Separator = ',', HelpText = "Comma separated list of Integer ids of data sources.")]
    public IEnumerable<int>? SourceIds { get; set; }
    
    [Option('a', "code all data types", Required = false, HelpText = "If present, causes coding of unmatched (=1) or all (=2) types of codable data")]
    public int? RecodeAll { get; set; }

    [Option('g', "code orgs", Required = false,
        HelpText = "If present, forces the (re)coding of of unmatched (=1) or all (=2) organisational data")]
    public int? RecodeOrgs { get; set; }
    
    [Option('l', "code locations", Required = false, HelpText = "If present, forces the (re)coding of of unmatched (=1) or all (=2) country and location data data")]
    public int? RecodeLocations { get; set; }
    
    [Option('t', "code topics", Required = false, HelpText = "If present, forces the (re)coding of of unmatched (=1) or all (=2) topic data")]
    public int? RecodeTopics { get; set; }
    
    [Option('c', "code conditions", Required = false, HelpText = "If present, forces the (re)coding of of unmatched (=1) or all (=2) conditions data")]
    public int? RecodeConditions { get; set; }
    
    [Option('p', "code publishers", Required = false, HelpText = "If present, forces the (re)coding of of unmatched (=1) or all (=2) publisher data")]
    public int? RecodePublishers { get; set; }
    
    [Option('M', "code test data only", Required = false, HelpText = "Only test data is recoded")]
    public bool ReCodeTestDataOnly { get; set; }
}


public class ParamsCheckResult
{
    internal bool ParseError { get; set; }
    internal bool ValidityError { get; set; }
    internal Options? Pars { get; set; }

    internal ParamsCheckResult(bool parseError, bool validityError, Options? pars)
    {
        ParseError = parseError;
        ValidityError = validityError;
        Pars = pars;
    }
}
