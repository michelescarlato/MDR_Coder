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

            if (opts.RecodeAll)    // recode ALL options force the recoding of all data, even if already coded
            {
                opts.RecodeAllOrgs = true;
                opts.RecodeAllConditions = true;
                opts.RecodeAllTopics = true;
                opts.RecodeAllLocations = true;
                opts.RecodeAllPublishers = true;
            }
            
            if (opts.RecodeUnmatchedAll)    // recode ALL options force the recoding of all data, even if already coded
            {
                opts.RecodeUnmatchedOrgs = true;
                opts.RecodeUnmatchedConditions = true;
                opts.RecodeUnmatchedTopics = true;
                opts.RecodeUnmatchedLocations = true;
                opts.RecodeUnmatchedPublishers = true;
            }
            
            // parameters valid - return opts.

            return new ParamsCheckResult(false, false, opts);
        }

        catch (Exception e)
        {
            _loggingHelper.OpenNoSourceLogFile();
            _loggingHelper.LogHeader("INVALID PARAMETERS");
            _loggingHelper.LogCommandLineParameters(opts);
            _loggingHelper.LogCodeError("Importer application aborted", e.Message, e.StackTrace);
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
    
    [Option('a', "code unmatched", Required = false, HelpText = "If present, forces the coding of all of the unmatched codable data")]
    public bool RecodeUnmatchedAll { get; set; }
    
    [Option('g', "code unmatched orgs", Required = false, HelpText = "If present, forces the (re)coding of all of the codable ad organisational data")]
    public bool RecodeUnmatchedOrgs { get; set; }
    
    [Option('l', "code unmatched locations", Required = false, HelpText = "If present, forces the (re)coding of all of the codable ad country and location data data")]
    public bool RecodeUnmatchedLocations { get; set; }
    
    [Option('t', "code unmatched topics", Required = false, HelpText = "If present, forces the (re)coding of all of the codable ad topic data")]
    public bool RecodeUnmatchedTopics { get; set; }
    
    [Option('c', "code unmatched conditions", Required = false, HelpText = "If present, forces the (re)coding of all of the codable ad conditions data")]
    public bool RecodeUnmatchedConditions { get; set; }
    
    [Option('p', "code unmatched publishers", Required = false, HelpText = "If present, forces the (re)coding of all of the codable ad publisher data")]
    public bool RecodeUnmatchedPublishers { get; set; }
    
    [Option('A', "code ALL", Required = false, HelpText = "If present, forces the (re)coding of all of the codable data")]
    public bool RecodeAll { get; set; }
    
    [Option('G', "code ALL orgs", Required = false, HelpText = "If present, forces the (re)coding of all of the codable ad organisational data")]
    public bool RecodeAllOrgs { get; set; }
    
    [Option('L', "code ALL locations", Required = false, HelpText = "If present, forces the (re)coding of all of the codable ad country and location data data")]
    public bool RecodeAllLocations { get; set; }
    
    [Option('T', "code ALL topics", Required = false, HelpText = "If present, forces the (re)coding of all of the codable ad topic data")]
    public bool RecodeAllTopics { get; set; }
    
    [Option('C', "code ALL conditions", Required = false, HelpText = "If present, forces the (re)coding of all of the codable ad conditions data")]
    public bool RecodeAllConditions { get; set; }
    
    [Option('P', "code ALL publishers", Required = false, HelpText = "If present, forces the (re)coding of all of the codable ad publisher data")]
    public bool RecodeAllPublishers { get; set; }
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
