import requests
from typing import Any, Annotated, Literal
from pydantic import BaseModel, ConfigDict, Field, PlainValidator, PrivateAttr
from pydantic.alias_generators import to_camel
import pytest

_VALID_FMT = "%Y-%m-%d"
_MODIFIED_FMT = "%Y-%m-%dT%H:%M:%S.%f%z"
_EXCLUDE_LINKS_T = Annotated[Any, Field(alias="_links", exclude=True)]
_HEADERS = {"Accept": "application/json;charset=UTF-8"}


class SSBModel(BaseModel, frozen=True):
    model_config = ConfigDict(
        alias_generator=to_camel, # Use camelCase for JSON keys
        extra="forbid",  # Forbid extra fields not defined in the model
        strict=True, # disable coercion of types, e.g. int to str
        frozen=True # make immutable (sorf of)
    )


class Changelog(SSBModel, frozen=True):
    """Undocumented in API"""
    change_occured: str
    description: str


class ContactPerson(SSBModel, frozen=True):
    """Undocumented in API"""
    name: str # Contact person name
    email: str # Contact person email
    phone: str # Contact person phone number


class Level(SSBModel, frozen=True):
    """Undocumented in API"""
    level_number: int # Hierarchical depth
    level_name: str # Name at depth


class VariantHeader(SSBModel, frozen=True):
    name: str # name of variant
    ID: int # ID  of variant
    contact_person: ContactPerson # contact person
    owning_section: str|None # Owning section
    last_modified: str # variant last modified
    published: list[str] # languages published in
    links: _EXCLUDE_LINKS_T


class CorrespondenceTableHeader(SSBModel, frozen=True):
    """Undocumented in API"""
    name: str # Correspondence table name
    ID: int # Correspondence table ID
    contact_person: ContactPerson # Contact person for correspondence table
    owning_section: str|None # Owning SSB section
    source: str # Source classification name
    source_id: int # Source classification ID
    target: str # Target classification name
    target_id: int # Target classification ID
    change_table: bool # If true, indicates that the correspondence table has a change table
    published: list[str] # A list of the languages the correspondence table is published in
    source_level: Level|None # Level name and number for the source classification version
    target_level: Level|None # Level name and number for the target classification version
    last_modified: str # Time and date the correspondence table was last time modified at
    links: _EXCLUDE_LINKS_T # Links to operations on the correspondence table (...)


class CorrespondenceTable(CorrespondenceTableHeader, frozen=True):
    """https://data.ssb.no/api/klass/v1/api-guide.html#_correspondencetables_by_id"""

    class CorrespondenceMap(SSBModel, frozen=True):
        source_code: str
        source_name: str
        target_code: str
        target_name: str
    
    description: str
    changelogs: list[Changelog]
    correspondence_maps: list[CorrespondenceMap]

    def __init__(self, ID: int):
        try:
            response = requests.get(f"https://data.ssb.no/api/klass/v1/correspondencetables/{ID}", headers=_HEADERS)
            response.raise_for_status()
            payload = response.json()
            super().__init__(**payload)
        except requests.exceptions.HTTPError:
            raise ValueError(f"Failed to fetch correspondence table with ID {ID}") from None
        except requests.exceptions.JSONDecodeError:
            raise ValueError(f"Failed to decode JSON response for correspondence table with ID {ID}") from None


class VersionHeader(SSBModel, frozen=True):
    """Undocumented in API"""
    name: str # Version name
    ID: int # Version ID
    valid_from: str # Date the version is valid from
    valid_to: str|None=None # date the version is valid to
    last_modified: str # Time and date the version was last time modified at
    published: list[str]  # List of languages the version is published in
    links: _EXCLUDE_LINKS_T # Links to operations on the version (aliased to avoid conflict with private attribute)


class Version(VersionHeader, frozen=True):
    """https://data.ssb.no/api/klass/v1/api-guide.html#_versions_by_id"""

    class Item(SSBModel, frozen=True):
        code: str # code
        parent_code: str # empty if top of hierarchy
        level: str # hierarchical depth
        name: str # name of code
        short_name: str|None # None if clasification include_short_name is False
        notes: str|None # None if classification include_notes is False

    introduction: str # Version description
    contact_person: ContactPerson # Contact person
    owning_section: str|None # Owning section
    legal_base: str # Legal base
    publications: str # Source references
    derived_from: str # Derived from
    correspondence_tables: list[CorrespondenceTableHeader] # List of correspondence tables for the version
    classification_variants: list[VariantHeader] # List of classification variants for the version
    changelogs: list[Changelog] # Log of changes
    levels: list[Level] # List of levels
    classification_items: list[Item] # Array of classification items

    def __init__(self, ID: int):
        try:
            response = requests.get(f"https://data.ssb.no/api/klass/v1/versions/{ID}", headers=_HEADERS)# params={"includeFuture": True})
            response.raise_for_status()
            payload = response.json()
            super().__init__(**payload)
        except requests.exceptions.HTTPError:
            raise ValueError(f"Failed to fetch classification version with ID {ID}") from None
        except requests.exceptions.JSONDecodeError:
            raise ValueError(f"Failed to decode JSON response for classification version with ID {ID}") from None


class Classification(SSBModel, frozen=True):
    """https://data.ssb.no/api/klass/v1/api-guide.html#classification"""
    name: str # Classification name
    ID: int # Classification ID
    description: str # Description of classification
    primary_language: str # Primary language for classification
    classification_type: str # Type of classification, Classification or Codelist
    copyrighted: bool # If true, classification is copyrighted
    include_short_name: bool # If true, indicates that classificationItems may have shortnames
    include_notes: bool # If true, indicates that classificationItems may have notes
    contact_person: ContactPerson # Contact person for classification (type guessed, not documented in API)
    owning_section: str|None # Owning SSB section
    statistical_units: list[str]  # Statistical units assigned to classification (type guessed, not documented in API)
    last_modified: str # Last time classification has been modified
    versions: list[VersionHeader] # Array of classification versions
    links: _EXCLUDE_LINKS_T # Links to operations on classification (...)


    def __init__(self, ID: int, include_future: bool = False):
        try:
            response = requests.get(f"https://data.ssb.no/api/klass/v1/classifications/{ID}", headers=_HEADERS, params={"includeFuture": include_future})
            response.raise_for_status()
            payload = response.json()
            super().__init__(**payload)
        except requests.exceptions.HTTPError:
            raise ValueError(f"Failed to fetch classification with ID {ID}") from None
        except requests.exceptions.JSONDecodeError:
            raise ValueError(f"Failed to decode JSON response for classification with ID {ID}") from None

