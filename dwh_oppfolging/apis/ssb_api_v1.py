"ssb api"

import logging
from datetime import timedelta
import requests # type: ignore
from collections import defaultdict
from dwh_oppfolging.apis.ssb_api_v1_types import (
    Version,
    Correspondence,
    Classification,
    CodeChangeItem,
)

API_VERSION = 1
API_NAME = "SSB"
SEKTOR_ID = 39
NAERING_ID = 6
YRKESKATALOG_ID = 145
YRKESKLASSIFISERING_ID = 7
YRKESKATALOG_TO_YRKESKLASSIFISERING_ID = 426
ORGANISASJONSFORM_ID = 35

_BASE_URL = f"https://data.ssb.no/api/klass/v{API_VERSION}"
_HEADERS = {"Accept": "application/json;charset=UTF-8"}


def get_classification(classification_id: int, include_future: bool = False):
    """
    Makes a get request to SSB Klass API and builds a Classification from the JSON response

    params:
        - classification_id: int
        - include_future: bool = False
            > If this is set then classification versions which become valid
            in the *future* are made available in the versions list.
    returns:
        Classification
    """
    url = _BASE_URL + f"/classifications/{classification_id}"
    params = {"includeFuture": include_future}
    response = requests.get(url, headers=_HEADERS, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    return Classification.from_json(data)


def get_classification_version(classification_id: int, version_id: int):
    """
    Makes a get request to SSB Klass API and builds a Version from the JSON response

    params:
        classification_id: int
        version_id: int
    returns:
        Version
    """
    url = _BASE_URL + f"/versions/{version_id}"
    response = requests.get(url, headers=_HEADERS, timeout=10)
    response.raise_for_status()
    data = response.json()
    return Version.from_json(data, classification_id)


def get_correspondence(source_classification_id: int, target_classification_id: int, correspondence_id: int):
    """
    Makes a get request to SSB Klass API and builds a Correspondence from the JSON response

    params:
        - source_classification_id: int
        - target_classification_id: int
        - correspondence_id: int
            > Note: It is not checked if the returned Correspondence is actually between the provided classifications.
            To validate this, check if Correspondence.source_version_id and Correspondence.target_version_id
            are in the source and target Classifications.versions list respectively.
            Alternatively, check if the correspondence_id is in the source and target Version.correspondence_tables,
            since correspondences are between specific versions of classifications.
    returns:
        Correspondence
    """
    url = _BASE_URL + f"/correspondencetables/{correspondence_id}"
    response = requests.get(url, headers=_HEADERS, timeout=10)
    response.raise_for_status()
    data = response.json()
    return Correspondence.from_json(data, source_classification_id, target_classification_id)


def get_changes_between_versions_in_classification(classification_id: int):
    """
    Makes a get request to SSB Klass API and builds list of CodeChangeItem from the JSON response.

    NB: if a change table is not available between successive versions
        then nothing is returned for that period.

    params:
        - classification_id: int
    returns:
        list[CodeChangeItem]
    """
    url = _BASE_URL + f"/classifications/{classification_id}/changes"

    classification = get_classification(classification_id)
    versions_sorted_asc = sorted((version for version in classification.versions), key=lambda x: x.valid_from)
    version_lkp = {version.valid_from: version.version_id for version in classification.versions}
    changes: list[CodeChangeItem] = []
    for idx, version in enumerate(versions_sorted_asc[:-1]): # skip end: the latest version cannot have changes to a later version...
        params = {
            "from": version.valid_from.date().isoformat(),
            # add a single day here because if to = next version's valid_from then for some reason nothing is returned
            # also, adding a to parameter gets rid of new code duplicates which otherwise appear in every older version
            "to": (versions_sorted_asc[idx + 1].valid_from.date() + timedelta(days=1)).isoformat()
        }
        response = requests.get(url, headers=_HEADERS, params=params, timeout=10)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            logging.warning(response.text)
            continue
        data = response.json()

        count_lkp: dict[str, int] = defaultdict(int) # we only count within the current verion
        for item in data["codeChanges"]:
            if item["oldCode"] is not None:
                count_lkp[item["oldCode"]] += 1

        changes.extend(
            CodeChangeItem.from_json(item, classification_id, version.version_id, version_lkp, count_lkp)
            for item in data["codeChanges"]
        )
    return changes