namespace MDR_Coder;

public interface IMonDataLayer
{
    Credentials Credentials { get; }

    bool SourceIdPresent(int? source_id);
    Source? FetchSourceParameters(int? source_id);
    int GetNextCodingEventId();

    int StoreCodingEvent(CodeEvent coding);
    
    // void LogDiffs(ISource s);
}

