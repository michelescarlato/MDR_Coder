
namespace MDR_Coder;

public interface ISource
{
    int? id { get; }
    string? source_type { get; }
    int? preference_rating { get; }
    string? database_name { get; }
    string? db_conn { get; set; }
    int? default_harvest_type_id { get; }
    bool? requires_file_name { get; }
    bool? uses_who_harvest { get; }
    int? harvest_chunk { get; }
    string? local_folder { get; }
    bool? local_files_grouped { get; }
    string? local_file_prefix { get; }
    int? grouping_range_by_id { get; }

    bool? has_study_tables { get; }
    bool? has_study_topics { get; }
    bool? has_study_features { get; }
    bool? has_study_contributors { get; }
    bool? has_study_countries { get; }
    bool? has_study_references { get; }
    bool? has_study_relationships { get; }
    bool? has_study_links { get; }
    bool? has_study_locations { get; }
    bool? has_study_ipd_available { get; }

    bool? has_object_datasets { get; }
    bool? has_object_dates { get; }
    bool? has_object_relationships { get; }
    bool? has_object_rights { get; }
    bool? has_object_pubmed_set { get; }
}