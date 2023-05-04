namespace MDR_Coder;

public interface IMonDataLayer
{
    Credentials Credentials { get; }

    bool SourceIdPresent(int? source_id);
    Source? FetchSourceParameters(int? source_id);
    int GetNextCodingEventId();
    int StoreCodingEvent(CodeEvent coding);

    void UpdateStudiesCodedDate(int codingId, string db_conn_string);
    void UpdateObjectsCodedDate(int codingId, string db_conn_string);

}

