"brreg api"
# pylint: disable=line-too-long
import logging
from typing import Generator
from datetime import datetime
import gzip
import requests # type: ignore
import ijson # type: ignore
from dwh_oppfolging.transforms.functions import json_to_string, string_to_sha256_hash, string_to_naive_norwegian_datetime
from dwh_oppfolging.apis.brreg_api_v1_types import PagedRequestForEmbeddedList, Update, PagedRequestError, UnpagedRequestForEmbeddedList


API_VERSION = 1
API_NAME = "BRREG"
UNIT_NAME_ENHET = "enheter"
UNIT_NAME_UNDERENHET = "underenheter"

_BASE_URL = "https://data.brreg.no/enhetsregisteret/api"
_HEADERS_FMT = "application/vnd.brreg.enhetsregisteret.{unit_name}.v" + str(API_VERSION) + "+{ftype};charset=UTF-8"


def _build_headers(unit_name: str, ftype: str = "json"):
    headers = {"Accept": _HEADERS_FMT.format(unit_name=unit_name[:-2], ftype=ftype)}
    return headers


def _convert_brreg_date(date: str | None):
    """converts brreg date string to datetime"""
    if date is None:
        return None
    converted_date = string_to_naive_norwegian_datetime(date.replace("Z", "+00:00"))
    return converted_date


def _build_update_struct_from_brreg_json(data: dict):
    """returns Update struct"""
    update = Update(
        data["organisasjonsnummer"],
        data["endringstype"],
        _convert_brreg_date(data["dato"]),
    )
    return update


def _build_unit_record(update: Update, fact: dict, download_date: datetime):
    """returns row in dict form
    NB: may delete field in fact"""
    fact.pop("_links", None)
    fact.get("organisasjonsform", {}).pop("_links", None)
    fact.pop("links", None)
    record = {}
    record["organisasjonsnummer"] = update.orgnr
    record["endringstype"] = update.change
    record["oppdatert_tid_kilde"] = update.last_modified
    record["api_versjon"] = API_VERSION
    record["data"] = json_to_string(fact)
    record["sha256_hash"] = string_to_sha256_hash(record["data"])
    record["lastet_dato"] = download_date
    record["kildesystem"] = API_NAME
    return record


def _get_unit_record_from_update_single(update: Update, unit_name: str, download_date: datetime):
    """returns a unit record
    makes a single request to get the latest fact for a single orgnr"""
    url = _BASE_URL + "/" + unit_name + "/" + update.orgnr
    headers = _build_headers(unit_name)
    response = requests.get(url, headers=headers, proxies={}, timeout=100)
    fact = response.json()
    return _build_unit_record(update, fact, download_date)


def _get_unit_record_from_update_batch(orgnr_update_lkp: dict[str, Update], unit_name: str, download_date: datetime, page_size: int = 500):
    """Returns {orgnr: record} constructed by searching for details for orgs in given {orgnr: update}
    NB: if an orgnr is found, it is popped from the orgnr_update_lkp input dictionary
    Org details will not be found if the org has been deleted at any point in the past,
    as this seems to remove them from the search endpoint.
    """
    logging.info(f"Making search request for detail/fact with orgnr param size: {len(orgnr_update_lkp)}")
    url = _BASE_URL + "/" + unit_name
    params = {
        "organisasjonsnummer": ",".join(orgnr for orgnr in orgnr_update_lkp),
        "sort": "organisasjonsnummer,ASC",
    }
    headers = _build_headers(unit_name)
    list_key = unit_name
    orgnr_record_lkp: dict = {}
    for facts in PagedRequestForEmbeddedList(url, params, headers, {}, page_size, list_key, timeout=100):
        orgnr_record_lkp |= {
            fact["organisasjonsnummer"]:
                _build_unit_record(orgnr_update_lkp.pop(fact["organisasjonsnummer"]), fact, download_date)
            for fact in facts
        }

    return orgnr_record_lkp


def get_latest_records_for_unit(
        download_date: datetime,
        last_modified_date: datetime,
        unit_name: str, page_size=500,
        orgnr_search: list[str] | None = None,
) -> Generator[list, None, None]:
    """params:
        last_modified_date: smallest update date
        unit_name: enheter | undereneheter
        orgnr: list[str] | None
    returns list of records
    
    If specified, orgnr is a list of orgnrs,
    and only these will be searced for. Every orgnr given this way
    must be associated with the given unit_name.
    NOTE: when searching for missing orgnr this way, it is recommended
    to set last_modified_date far back into the past so that you are
    guaranteed to find at least one update newer than it.
    """
    url = _BASE_URL + "/oppdateringer/" + unit_name
    headers = _build_headers("oppdatering." + unit_name)
    list_key = "oppdaterte" + unit_name.capitalize()
    params = {"dato": last_modified_date.isoformat(timespec="milliseconds") + "Z"}
    if orgnr_search is not None:
        params |= {"organisasjonsnummer": ",".join(orgnr_search)}
    orgnr_record_lkp: dict = {} # {orgnr from latest update for that orgnr: latest fact for that orgnr}

    try:
        iter_obj = PagedRequestForEmbeddedList(url, params, headers, {}, page_size, list_key, timeout=100)
    except PagedRequestError:
        logging.warning("Using unpaged request, as paged request will fail due to BRREG API limitations")
        iter_obj = UnpagedRequestForEmbeddedList(url, params, headers, {}, size=page_size, list_key=list_key, timeout=100) # type: ignore

    for updates in iter_obj:
        # only keep the latest updates for each orgnr
        # since updates is already sorted with ascending update id, we can just overwrite
        orgnr_update_lkp = {update.orgnr: update for update in map(_build_update_struct_from_brreg_json, updates)}

        old_length = len(orgnr_update_lkp)
        orgnr_record_lkp_batch = _get_unit_record_from_update_batch(orgnr_update_lkp, unit_name, download_date)
        new_length = len(orgnr_update_lkp)
        logging.info(f"found {new_length} / {old_length} orgs with batched search, remaining will be appened using single requests")
        orgnr_record_lkp_batch |= {
            orgnr: _get_unit_record_from_update_single(update, unit_name, download_date)
            for orgnr, update in orgnr_update_lkp.items()
        }

        orgnr_record_lkp |= orgnr_record_lkp_batch

    # handle orgnr we didn't find any updates for
    # possibly because it was before update types were introduced, or it has been deleted..
    if orgnr_search is not None:
        missing_orgnrs = set(orgnr_search) - orgnr_record_lkp.keys()
        if len(missing_orgnrs) > 0:
            logging.warning(f"Found no updates for {len(missing_orgnrs)} requested orgnrs. Facts will be downloaded directly, updates set to unknown")
            orgnr_record_lkp |= {
                orgnr: _get_unit_record_from_update_single(
                            Update(orgnr, "UKJENT", last_modified_date),
                            unit_name, download_date,
                        )
                for orgnr in missing_orgnrs
            }
        else:
            logging.info("All requested orgnrs found.")
    yield list(orgnr_record_lkp.values())


def stream_latest_records_for_unit_from_file(download_date: datetime, last_modified_date: datetime, unit_name: str, batch_size: int = 1000) -> Generator[list, None, None]:
    """
    yields [records] from large file, must be run some time after 5 brreg local time
    useful for init loading table
    """
    #updated_date_str = last_modified_date.isoformat(timespec="milliseconds")+"Z"
    url = _BASE_URL + "/" + unit_name + "/" + "lastned"
    headers = _build_headers(unit_name, "gzip")
    logging.info("requesting filestream from api")
    with requests.get(url, headers=headers, proxies={}, stream=True, timeout=100) as response:
        response.raise_for_status()
        logging.info("decompressing filestream")
        with gzip.open(response.raw, "rb") as file:
            records = []
            logging.info("iterating over json objects")
            for record in ijson.items(file, "item"):  # type: ignore
                records.append(
                    _build_unit_record(
                        Update(
                            record["organisasjonsnummer"],
                            "FLATFIL",
                            last_modified_date,
                        ),
                        record,
                        download_date,
                    )
                )
                if len(records) >= batch_size:
                    yield records
                    records = []
            if len(records) > 0:
                yield records
