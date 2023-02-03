namespace MDR_Coder;

public interface IMonDataLayer
{
    Credentials Credentials { get; }

    bool SourceIdPresent(int? source_id);
    Source? FetchSourceParameters(int? source_id);
    int GetNextImportEventId();

    IEnumerable<StudyFileRecord> FetchStudyFileRecords(int? source_id, int harvest_type_id = 1, DateTime? cutoff_date = null);
    IEnumerable<ObjectFileRecord> FetchObjectFileRecords(int? source_id, int harvest_type_id = 1, DateTime? cutoff_date = null);

    int FetchFileRecordsCount(int? source_id, string source_type, int harvest_type_id = 1, DateTime? cutoff_date = null);

    IEnumerable<StudyFileRecord> FetchStudyFileRecordsByOffset(int? source_id, int offset_num, 
                                  int amount, int harvest_type_id = 1, DateTime? cutoff_date = null);
    IEnumerable<ObjectFileRecord> FetchObjectFileRecordsByOffset(int? source_id, int offset_num,
                                         int amount, int harvest_type_id = 1, DateTime? cutoff_date = null);

    StudyFileRecord? FetchStudyFileRecord(string sd_id, int? source_id, string source_type);
    ObjectFileRecord? FetchObjectFileRecord(string sd_id, int? source_id, string source_type);

    void UpdateFileRecLastImported(int id, string source_type);
    int StoreImportEvent(ImportEvent import);
    bool CheckIfFullHarvest(int? source_id);

    void UpdateStudiesLastImportedDate(int import_id, int? source_id);

    void UpdateObjectsLastImportedDate(int import_id, int? source_id);
    // void LogDiffs(ISource s);
}

