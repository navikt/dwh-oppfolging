"Datatypes used by brreg api"

import logging
from collections import namedtuple
import requests # type: ignore

Update = namedtuple("Update", ["orgnr", "change", "last_modified"])


class PagedRequestError(Exception):
    """error for pagedrequestforembeddedlist class"""


class PagedRequestForEmbeddedList:
    """brreg api endpoint page"""
    def __init__(
        self,
        url: str,
        params: dict,
        headers: dict,
        proxies: dict,
        page_size: int,
        list_key: str,
        timeout: float,
    ) -> None:
        self.url = url
        self.headers = headers
        self.params = params
        self.proxies = proxies
        self.page_number = 0
        self.total_elements_read = 0
        self.page_size = page_size
        self.list_key = list_key
        self.timeout = timeout

        # check
        self.__iter__()
        response = requests.get(
            self.url,
            self.params,
            headers=self.headers,
            proxies=self.proxies,
            timeout=self.timeout,
        )
        try:
            page = response.json()["page"]
            assert page["size"] * page["totalPages"] < 10000
        except AssertionError as exc:
            raise PagedRequestError(
                "This request will fail due to BRREG API limitations"
                ) from exc

    def __iter__(self):
        self.page_number = 0
        self.total_elements_read = 0
        self.params["size"] = self.page_size
        self.params["page"] = self.page_number
        return self

    def __next__(self) -> list:
        self.params["page"] = self.page_number
        response = requests.get(
            self.url,
            self.params,
            headers=self.headers,
            proxies=self.proxies,
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        try:
            elements = data["_embedded"][self.list_key]
            assert len(elements) > 0
        except (KeyError, AssertionError) as exc:
            raise StopIteration from exc
        self.page_number += 1
        self.total_elements_read += len(elements)
        logging.info(
            f"{self.url}:"
            + f" found {len(elements)} on page {data['page']['number']}/{data['page']['totalPages'] - 1}"
            + f" (total: {self.total_elements_read}/{data['page']['totalElements']})"
        )
        return elements


class UnpagedRequestForEmbeddedList:
    """ brreg api endpoint using update id"""
    def __init__(
        self,
        url: str,
        params: dict,
        headers: dict,
        proxies: dict,
        size: int,
        list_key: str,
        timeout: float,
    ) -> None:
        self.url = url
        self.headers = headers
        self.params = params
        self.proxies = proxies
        self.total_elements_read = 0
        self.list_key = list_key
        self.timeout = timeout
        self.update_id = 1
        self.size = size

    def __iter__(self):
        self.update_id = 1
        self.total_elements_read = 0
        self.params["size"] = self.size
        return self

    def __next__(self):
        self.params["oppdateringsid"] = self.update_id
        response = requests.get(
            self.url,
            self.params,
            headers=self.headers,
            proxies=self.proxies,
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        try:
            elements = data["_embedded"][self.list_key]
            assert len(elements) > 0
        except (KeyError, AssertionError) as exc:
            logging.info(f"List exhausted at {self.update_id}, total read: {self.total_elements_read}")
            raise StopIteration from exc
        logging.info(
            f"{self.url}:"
            + f" found {len(elements)} with update id >= {self.update_id}"
        )
        self.update_id = elements[-1]["oppdateringsid"] + 1
        self.total_elements_read += len(elements)
        return elements
