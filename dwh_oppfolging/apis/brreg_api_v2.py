"brreg api"
# pylint: disable=line-too-long
import logging
from typing import Generator, Literal
from datetime import datetime
import gzip
import requests # type: ignore
import ijson # type: ignore
from dwh_oppfolging.transforms.functions import (
    json_to_string,
    string_to_sha256_hash,
    string_to_naive_norwegian_datetime,
)

API_VERSION = 2
API_NAME = "BRREG"


class UnitNotFoundError(Exception):
    """Unit not found"""


class UnitHasNoUpdatesError(Exception):
    """Unit has no updates"""


class BrregUnitAPI:
    """Base class for BRREG endpoints of Enheter and Underenheter"""
    def __init__(self, unit_type: Literal['Enhet', 'Underenhet'], download_date: datetime) -> None:
        """
        Creates a new BRREGUnitAPI instance.

        Params:
            - unit_type: of of 'Enhet' or 'Underenhet', determines unit endpoints used
            - download_date: naive norwegian datetime, used for row generation
        """

        self._unit_fetch_headers = self._format_headers(unit_type.lower(), "json", API_VERSION)
        self._unit_update_headers = self._format_headers("oppdatering." + unit_type.lower(), "json", 1)
        self._unit_file_headers = self._format_headers(unit_type.lower(), "gzip", API_VERSION)

        base_url = "https://data.brreg.no/enhetsregisteret/api"

        self._unit_fetch_url = base_url + "/" + unit_type.lower() + "er"
        self._unit_update_url = base_url + "/" + "oppdateringer" + "/" + unit_type.lower() + "er"
        self._unit_updates_list_key = "oppdaterte" + unit_type + "er"
        self._unit_file_url = base_url + "/" + unit_type.lower() + "er" + "/" + "lastned"

        self.download_date = download_date

    def set_row_download_date(self, date: datetime):
        """
        Resets the download date used when making rows from the units.

        Params:
            - date: naive norwegian datetime
        """
        self.download_date = date


    def _format_headers(self, name, rtype, api_version):
        """api version must be 1 for update endpoints 09.23"""
        return {"Accept": f"application/vnd.brreg.enhetsregisteret.{name}.v{api_version}+{rtype};charset=UTF-8"}


    def brreg_date_to_naive_norwegian_datetime(self, date: str | None):
        """
        Converts brreg date string to naive norwegian datetime.
        
        Params:
            - date: date string in brreg format
        
        Returns:
            - naive norwegian datetime
        """
        if date is None:
            return None
        converted_date = string_to_naive_norwegian_datetime(date.replace("Z", "+00:00"))
        return converted_date


    def naive_utc0_datetime_to_brreg_date_str(self, date: datetime):
        """
        Converts naive utc0 datetime to brreg formatted date string.
        
        Params:
            - date: naive UTC0 datetime
        
        Returns:
            - BRREG date string that can be used in the BRREG API
        """
        date_str = date.isoformat(timespec="milliseconds") + "Z"
        return date_str


    def make_fake_unit(self, orgnr: str):
        """
        Makes a fake unit json document

        Params:
            - orgnr: 9-digit organization string

        Returns:
            - json document
        """
        return {"organisasjonsnummer": orgnr}


    def get_unit(self, orgnr: str, fake_if_not_found: bool = False):
        """
        Makes a get request to fetch the unit with given orgnr.

        If fake_if_not_found is True and the unit is not found,
        a faked unit document is returned.

        Params:
            - orgnr: 9-digit organization string
            - fake_if_not_found: bool

        Raises:
            - UnitNotFoundError

        Returns:
            - json document
        """
        response = requests.get(self._unit_fetch_url + "/" + orgnr, headers=self._unit_fetch_headers, timeout=100)
        try:
            response.raise_for_status()
        except requests.HTTPError:
            # Note: According to the BRREG API documentation, status code 410 is used for deleted orgnr, but we actually get 404.
            logging.warning(f"orgnr {orgnr} may have been deleted")
        try:
            document = response.json()
        except requests.JSONDecodeError as exc:
            if fake_if_not_found:
                logging.warning(f"orgnr {orgnr} is entirely gone, faking json document")
                document = self.make_fake_unit(orgnr)
            else:
                raise UnitNotFoundError(f"{orgnr}") from exc
        return document


    def make_fake_unit_update(
        self,
        orgnr: str,
        change: str = 'UKJENT',
        last_modified_date: datetime = datetime(1899, 12, 31, 23),
    ):
        """
        Creates a fake unit update json document.
        Useful for units which have no update history.

        Params:
            - orgnr: 9-digit organization string
            - last_modified_date: naive UTC0 datetime
                default such that row will be generated with datetime 01.01.1900

        Returns:
            - json document
        """
        document = {
            "organisasjonsnummer": orgnr,
            "endringstype": change,
            "dato": self.naive_utc0_datetime_to_brreg_date_str(last_modified_date)
        }
        return document


    def get_unit_update_history(
        self,
        orgnr: str,
        latest_only: bool = False,
        fake_if_not_found: bool = False
    ):
        """
        Makes a get request to fetch all the updates on the unit with the given orgnr.

        If latest_only is True, then only the latest update is kept in the return.

        If fake_if_not_found is True and no updates are found, a faked update is made.

        Params:
            - orgnr: 9-digit organization string
            - latest_only: bool

        Raises:
            - HTTPError
            - UnitHasNoUpdatesError

        Returns:
            - list of json documents
        """
        params = {"organisasjonsnummer": orgnr, "oppdateringsid": 1}
        all_updates = []
        while True:
            response = requests.get(self._unit_update_url, headers=self._unit_update_headers, params=params, timeout=100)
            response.raise_for_status()
            document = response.json()
            try:
                updates = document["_embedded"][self._unit_updates_list_key]
                assert isinstance(updates, list) and len(updates) > 0
            except (KeyError, AssertionError):
                logging.info(f"No further updates found on orgnr {orgnr}")
                break
            # Note: According to the BRREG API documentation, filtering on updateid + 1 is safe (it is also sorted ascending).
            params["oppdateringsid"] = updates[-1]["oppdateringsid"] + 1
            all_updates.extend(updates)

        if len(all_updates) == 0:
            if fake_if_not_found:
                logging.warning(f"orgnr {orgnr} has no updates, faking update")
                all_updates.append(self.make_fake_unit_update(orgnr))
            else:
                raise UnitHasNoUpdatesError(f"{orgnr}")
        if latest_only:
            return all_updates[-1:]
        return all_updates


    def get_all_updates_since(
        self,
        last_modified_date: datetime,
        latest_only: bool = False
    ):
        """
        Makes a get request to fetch all the updates on any units since the provided datetime.

        If latest_only is True, then only the latest update is kept for each unit.

        NOTE: The returned dictionary may be quite large.

        Params:
            - last_modified_date: naive datetime in UTC0 (+Z) (+00:00)
            - latest_only: bool

        Raises:
            - HTTPError

        Returns:
            - a dict of {orgnr: [update]}
        """
        orgnr_update_map = {}
        params = {"dato": self.naive_utc0_datetime_to_brreg_date_str(last_modified_date), "oppdateringsid": 1}
        while True:
            response = requests.get(self._unit_update_url, headers=self._unit_update_headers, params=params, timeout=100)
            response.raise_for_status()
            document = response.json()
            try:
                updates = document["_embedded"][self._unit_updates_list_key]
                assert isinstance(updates, list) and len(updates) > 0
            except (KeyError, AssertionError):
                logging.info("No further updates found")
                break
            # Note: According to the BRREG API documentation, filtering on updateid + 1 is safe (it is also sorted ascending).
            params["oppdateringsid"] = updates[-1]["oppdateringsid"] + 1

            if latest_only:
                orgnr_update_map.update((update["organisasjonsnummer"], [update]) for update in updates)
            else:
                for update in updates:
                    orgnr = update["organisasjonsnummer"]
                    if orgnr in orgnr_update_map:
                        orgnr_update_map[orgnr].append(update)
                    else:
                        orgnr_update_map[orgnr] = [update]

        return orgnr_update_map


    def make_row(self, update: dict, fact: dict):
        """
        Combines the update and fact json documents (dicts) returned for each orgnr
        from the get_unit_update_history/get_all_unit_updates_since and get_unit
        methods, respectively.

        NOTE: This method may remove keys in the input dicts.

        Params:
            - update: dict
            - fact: dict
            - download_date: naive norwegian datetime

        Returns:
            - dict
        """
        fact.pop("_links", None)
        fact.pop("links", None)
        fact.get("organisasjonsform", {}).pop("_links", None)
        fact.get("organisasjonsform", {}).pop("links", None)
        record = {}
        record["organisasjonsnummer"] = update["organisasjonsnummer"]
        record["endringstype"] = update["endringstype"]
        record["oppdatert_tid_kilde"] = self.brreg_date_to_naive_norwegian_datetime(update["dato"])
        record["api_versjon"] = API_VERSION
        record["data"] = json_to_string(fact)
        record["sha256_hash"] = string_to_sha256_hash(record["data"])
        record["lastet_dato"] = self.download_date
        record["kildesystem"] = API_NAME
        return record

    def get_unit_as_row(
        self,
        orgnr: str,
        fake_update_if_not_found: bool = False,
        fake_unit_if_not_found: bool = False,
    ):
        """
        Gets unit data as row using get_unit_update_history and get_unit methods.
        Only the latest update for each unit is used to make the row.
        This method is useful for finding organizations that are referenced by others,
        but haven't been updated in a long while.

        Params:
            - orgnr: 9-digit organization strings
            - fake_update_if_not_found: bool
            - fake_unit_if_not_found: bool

        Returns:
            - dict
        """
        update = self.get_unit_update_history(orgnr, True, fake_update_if_not_found)[0]
        fact = self.get_unit(orgnr, fake_unit_if_not_found)
        row = self.make_row(update, fact)
        return row


    def get_all_units_as_rows_updated_since(
        self,
        last_modified_date: datetime,
        fake_unit_if_not_found: bool = False
    ):
        """
        Gets unit data as rows using get_all_updates_since and get_unit methods.
        Only the latest update for each unit is used to make the rows.

        If fake_unit_if_not_found is True then a faked unit is used in the row
        if the unit is gone despite an update having been found.
        This situation seems unlikely.

        Params:
            - last_modified_date: naive datetime in UTC0 (+Z) (+00:00)
            - fake_unit_if_not_found: bool

        Returns:
            - list of rows as dicts
        """

        orgnr_update_map = self.get_all_updates_since(last_modified_date, True)

        rows = [
            self.make_row(updates[0], self.get_unit(orgnr, fake_unit_if_not_found))
            for orgnr, updates in orgnr_update_map.items()
        ]

        return rows


    def stream_all_units_as_rows_from_file(
            self,
            batch_size: int = 1000,
        ) -> Generator[list, None, None]:
        """
        Yields lists of rows from a large file available in the BRREG API.
        Must be run some time after 5 brreg local time on a given day.
        Useful for init loading table.

        NOTE: The file API does not give any information about updates,
        so all updates are faked in making rows.

        Params:
            - download_date: naive norwegian datetime

        Yields:
            - list of dicts
        """
        logging.info("requesting filestream from api")
        url = self._unit_file_url
        headers = self._unit_file_headers
        with requests.get(url, headers=headers, stream=True, timeout=100) as response:
            response.raise_for_status()
            logging.info("decompressing filestream")
            with gzip.open(response.raw, "rb") as file:
                records = []
                logging.info("iterating over json objects")
                for record in ijson.items(file, "item"):  # type: ignore
                    records.append(
                        self.make_row(
                            self.make_fake_unit_update(record["organisasjonsnummer"],"FLATFIL"),
                            record,
                        )
                    )
                    if len(records) >= batch_size:
                        yield records
                        records = []
                if len(records) > 0:
                    yield records
