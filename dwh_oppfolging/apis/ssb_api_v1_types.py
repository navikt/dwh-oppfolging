"Datatypes used by ssb api"

from typing import NamedTuple, TypedDict
from datetime import datetime
from typing_extensions import Self
from dwh_oppfolging.transforms.functions import string_to_naive_norwegian_datetime, json_to_string, string_to_sha256_hash


_VALID_DATE_FMT = "%Y-%m-%d"
_MODIFIED_DATE_FMT = "%Y-%m-%dT%H:%M:%S.%f%z"


class CorrespondenceMap(TypedDict):
    """CorrespondenceMap in SSB API"""
    sourceCode: str
    sourceName: str
    targetCode: str
    targetName: str


class ClassificationItem(TypedDict):
    """ClassificationItem in SSB API"""
    code: str
    parentCode: str # key may not exist if code has no parent
    level: int
    name: str
    shortName: str # key may not exist if includeShortName is False
    notes: str # key may not exist if includeNotes is False


class Level(TypedDict):
    """Level item in SSB API"""
    levelNumber: int
    levelName: str


class CorrespondenceHeader(NamedTuple):
    """Correspondence Header as appearing in versions' correspondenceTables"""
    name: str
    """Correspondence name"""
    owning_section: str
    """Owning section of correspondence"""
    source_version_name: str
    """Source version name"""
    source_version_id: int
    """Source version identifier"""
    target_version_name: str
    """Target version name"""
    target_version_id: int
    """Target version identifier"""
    change_table: bool
    """..."""
    last_modified: datetime
    """Last modified date (changes may be invisible to API)"""
    self_url: str
    """Link to correspondence"""
    source_url: str
    """Link to source version"""
    target_url: str
    """Link to target version"""

    @classmethod
    def from_json(cls, data: dict):
        """Constructs CorrespondenceHeader from an entry in the json-version's correspondenceTables"""
        self_url = data["_links"]["self"]["href"]
        source_url = data["_links"]["source"]["href"]
        target_url = data["_links"]["target"]["href"]
        last_modified = string_to_naive_norwegian_datetime(data["lastModified"])
        return CorrespondenceHeader(
            data["name"], data["owningSection"], data["source"], data["sourceId"], data["target"],
            data["targetId"], data["changeTable"], last_modified, self_url, source_url, target_url
        )


class Correspondence(NamedTuple):
    """Correspondence Table model /correspondencetables/*"""
    name: str
    """Correspondence name"""
    owning_section: str
    """Owning section of correspondence"""
    source_version_name: str
    """Source version name"""
    source_version_id: int
    """Source version identifier"""
    source_classification_id: int
    """Source classification indentifier"""
    target_version_name: str
    """Target version name"""
    target_version_id: int
    """Target version identifier"""
    target_classification_id: int
    """Target classification identifier"""
    last_modified: datetime
    """Last modified date (changes may be invisible to API)"""
    correspondence_maps: list[CorrespondenceMap]
    """List of correspondening source- and target version codes"""
    @classmethod
    def from_json(cls, data: dict, source_classification_id: int, target_classification_id: int) -> Self:
        """Constructs Correspondence from json-correspondence"""
        last_modified = string_to_naive_norwegian_datetime(data["lastModified"])
        return cls(
            data["name"], data["owningSection"], data["source"], data["sourceId"],
            source_classification_id, data["target"], data["targetId"], target_classification_id,
            last_modified, data["correspondenceMaps"]
        )
    
    def to_records(self, api_version: int, api_name: str, download_date: datetime) -> list[dict]:
        """Converts Correspondence to database records"""
        records = []
        for correspondence_map in self.correspondence_maps:
            record: dict = {}
            record["fra_klassifikasjon_kode"] = str(self.source_classification_id)
            record["fra_versjon_kode"] = str(self.source_version_id)
            record["til_klassifikasjon_kode"] = str(self.target_classification_id)
            record["til_versjon_kode"] = str(self.target_version_id)
            record["oppdatert_tid_kilde"] = self.last_modified
            record["api_versjon"] = api_version
            record["data"] = json_to_string(correspondence_map)
            record["sha256_hash"] = string_to_sha256_hash(record["data"])
            record["lastet_dato"] = download_date
            record["kildesystem"] = api_name
            records.append(record)
        return records


class VersionHeader(NamedTuple):
    """Classification Version Header as appearing in classifications' classificationVersions"""
    name: str
    """Name of the version"""
    valid_from: datetime
    """Functional valid from date"""
    valid_to: datetime | None
    """Functional valid to date (None if valid now)"""
    last_modified: datetime
    """Last modified date (changes may be invisible to API)"""
    url: str
    """URL to full version data"""
    version_id: int
    """Version identifier"""

    @classmethod
    def from_json(cls, data: dict) -> Self:
        """Constructs VersionHeader from an entry in the json-classification's versions list"""
        url: str = data["_links"]["self"]["href"]
        version_id: int = int(url[url.rfind("/")+1:])
        #valid_from = string_to_naive_norwegian_datetime(data["validFrom"])
        #valid_to = string_to_naive_norwegian_datetime(data["validTo"]) if "validTo" in data else None
        # trunc valid dates, these are in form yyyy-mm-dd, and should not have time of day
        valid_from = datetime.strptime(data["validFrom"], _VALID_DATE_FMT)
        valid_to = datetime.strptime(data["validTo"], _VALID_DATE_FMT) if "validTo" in data else None
        last_modified = string_to_naive_norwegian_datetime(data["lastModified"])
        return cls(data["name"], valid_from, valid_to, last_modified, url, version_id)


class Classification(NamedTuple):
    """Classification Model /classifications/*"""
    name: str
    """Classification name"""
    last_modified: datetime
    """Last modified date (changes may be invisible to API)"""
    description: str
    """Description of classification"""
    includeShortName: bool
    """If shortName exists in implementing version codes"""
    includeNotes: bool
    """if Notes field exists in implementing version codes"""
    statistical_units: list[str]
    """List of statistical units in classification"""
    owning_section: str
    """Owning section of classification"""
    versions: list[VersionHeader]
    """List of versions (VersionHeader) in classification"""
    classification_id: int
    """Classification identifier"""

    @classmethod
    def from_json(cls, data: dict) -> Self:
        """Constructs Classification from json-classification"""
        versions = [VersionHeader.from_json(version) for version in data["versions"]]
        last_modified =  string_to_naive_norwegian_datetime(data["lastModified"])
        url = data["_links"]["self"]["href"]
        classification_id = int(url[url.rindex("/") + 1 :])
        return cls(
            data["name"], last_modified, data["description"],
            data["includeShortName"], data["includeNotes"], data["statisticalUnits"],
            data["owningSection"], versions, classification_id
        )
    
    def to_records(self, api_version: int, api_name: str, download_date: datetime) -> list[dict]:
        """Converts Classification to records"""
        raise NotImplementedError


class Version(NamedTuple):
    """Classification Version Model /versions/*"""
    name: str
    """name of classification version"""
    valid_from: datetime
    """functional valid from date"""
    valid_to: datetime | None
    """functional valid to date (or None if valid now)"""
    last_modified: datetime
    """Last modified date (changes may be invisible to API)"""
    introduction: str
    """Introductory description of the classification version"""
    classification_id: int
    """identifier of the classification this version implements"""
    version_id: int
    """Version identifier"""
    owning_section: str
    """Owning section of version"""
    derived_from: str | None
    """Standards and/or related groupings this version is derived from"""
    changelogs: list[str] | None
    """List of short descriptions of changes made to this version"""
    levels: list[Level]
    """List of levels, their index and name"""
    classification_items: list[ClassificationItem]
    """List of codes in this version"""
    correspondence_tables: list[CorrespondenceHeader]
    """list of correspondence table headers"""

    @classmethod
    def from_json(cls, data: dict, classification_id: int):
        """Constructs Version from json-version"""
        last_modified = string_to_naive_norwegian_datetime(data["lastModified"])
        #valid_from = string_to_naive_norwegian_datetime(data["validFrom"])
        #valid_to = string_to_naive_norwegian_datetime(data["validTo"]) if "validTo" in data else None
        # trunc valid dates, these are in form yyyy-mm-dd, and should not have time of day
        valid_from = datetime.strptime(data["validFrom"], _VALID_DATE_FMT)
        valid_to = datetime.strptime(data["validTo"], _VALID_DATE_FMT) if "validTo" in data else None
        corr_tables = [CorrespondenceHeader.from_json(corr) for corr in data["correspondenceTables"]]
        url = data["_links"]["self"]["href"]
        version_id = int(url[url.rindex("/") + 1 :])
        return cls(
            data["name"], valid_from, valid_to, last_modified, data["introduction"], classification_id,
            version_id, data["owningSection"], data.get("derivedFrom"), data.get("changelogs"), data["levels"],
            data["classificationItems"], corr_tables
            )

    def to_records(self, api_version: int, api_name: str, download_date: datetime) -> list[dict]:
        """Converts Version codes to records"""
        records = []
        for classification_item in self.classification_items:
            record: dict = {}
            record["klassifikasjon_kode"] = str(self.classification_id)
            record["versjon_kode"] = str(self.version_id)
            record["gyldig_fom_tid_kilde"] = self.valid_from
            record["gyldig_til_tid_kilde"] = self.valid_to
            record["oppdatert_tid_kilde"] = self.last_modified
            record["api_versjon"] = api_version
            record["data"] = json_to_string(classification_item)
            record["sha256_hash"] = string_to_sha256_hash(record["data"])
            record["lastet_dato"] = download_date
            record["kildesystem"] = api_name
            records.append(record)
        return records

    def to_metadata_record(self, api_version: int, api_name: str, download_date: datetime) -> dict:
        """Converts Version to metadata record"""
        record: dict = {}
        record["versjon_kode"] = str(self.version_id)
        record["klassifikasjon_kode"] = str(self.classification_id)
        record["versjon_besk"] = self.introduction
        record["versjon_navn"] = self.name
        record["utledet_fra_besk"] = self.derived_from
        record["nivaa_antall"] = len(self.levels)
        record["oppdatert_tid_kilde"] = self.last_modified
        record["gyldig_fom_tid_kilde"] = self.valid_from
        record["gyldig_til_tid_kilde"] = self.valid_to
        record["gyldig_flagg_kilde"] = self.valid_to is None
        record["oppdatert_dato"] = download_date
        record["lastet_dato"] = download_date
        record["kildesystem"] = api_name
        return record
