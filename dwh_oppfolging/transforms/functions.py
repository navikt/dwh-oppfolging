"data transforms"
from typing import Any
from functools import reduce
from datetime import datetime
import hashlib
import json
import re
import pendulum.parser
from pendulum.datetime import DateTime as PendulumDateTime


def find_in_dict(mapping: dict, path: list) -> Any | None:
    """
    recursively searches for value at path in dict
    returns value if found, None otherwise
    >>> find_in_dict({0: {1: 2}}, [0, 1])
    2
    >>> find_in_dict({}, [0, 1, 2])
    """
    try:
        return reduce(lambda d,k: d.get(k), path, mapping) # type: ignore
    except Exception: # pylint: disable=broad-except
        return None


def flatten_dict(mapping: dict, sep: str = "_", flatten_lists: bool = False) -> dict[str, Any]:
    """
    recursively flattens dict with specified separator
    optionally flatten lists in it as well
    note: all keys become strings
    >>> flatten_dict({0: {1: 3}, 'z': [1, 2, 3]}, "_", True)
    {'0_1': 3, 'z_0': 1, 'z_1': 2, 'z_2': 3}
    """
    def flatten(mapping: dict, parent_key: str = "") -> dict:
        items: list[Any] = []
        for key, value in mapping.items():
            flat_key = str(key) if not parent_key else str(parent_key) + sep + str(key)
            if isinstance(value, dict):
                items.extend(flatten(value, flat_key).items())
            elif isinstance(value, list) and flatten_lists:
                for it_key, it_value in enumerate(value):
                    items.extend(flatten({str(it_key):it_value}, flat_key).items())
            else:
                items.append((flat_key, value))
        return dict(items)

    return flatten(mapping)


def string_to_naive_norwegian_datetime(
    string: str,
    fmt: str | None = None
) -> datetime:
    """
    Parses string to pendulum datetime, then converts to Norwegian timezone
    (adjusting and adding utc offset, then appending tzinfo) and finally strips the timezone.

    params:
        - string: the string representing some date and time
        - (optional) fmt: an explicit date and time formatting string using pendulumn tokens to parse with
            note: some timezones and tokens may fail, this is an open issue in pendulum.
    returns:
        - pendulum datetime

    example: adjust from incoming timestamp assumed to be at 0 hours, +00:00 UTC
    >>> string_to_naive_norwegian_datetime("2022-05-05").isoformat()
    '2022-05-05T02:00:00'

    example: adjust from incoming timestamp at +00:00 UTC
    >>> string_to_naive_norwegian_datetime("1900-01-01T00:00:00+00:00").isoformat()
    '1900-01-01T01:00:00'

    example: adjust from incoming timezone where day changes, +00:00 UTC
    >>> string_to_naive_norwegian_datetime("1900-01-01T23:00:00+00:00").isoformat()
    '1900-01-02T00:00:00'

    example: adjust from incoming timezone where year changes, +00:00 UTC
    >>> string_to_naive_norwegian_datetime("1900-12-31T23:00:00+00:00").isoformat()
    '1901-01-01T00:00:00'

    example: adjust from incoming timezone where year changes, +00:00 UTC
    (this breaks with pendulum < 3.0.0)
    >>> string_to_naive_norwegian_datetime("1899-12-31T23:00:00+00:00").isoformat()
    '1900-01-01T00:00:00'

    example: specifying the optional format
    >>> string_to_naive_norwegian_datetime("Mon Jan 22 04:21:09 CET 2024", "ddd MMM DD HH:mm:ss z YYYY").isoformat()
    '2024-01-22T04:21:09'
    """
    pdl_dt = pendulum.parser.parse(string) if not fmt else pendulum.from_format(string, fmt)
    assert isinstance(pdl_dt, PendulumDateTime)
    pdl_dt = pdl_dt.in_timezone("Europe/Oslo")
    pdl_dt = pdl_dt.naive()
    return pdl_dt


def string_to_naive_utc0_datetime(
    string: str
) -> datetime:
    """
    Parses string to pendulum datetime, then converts to UTC timezone
    (adjusting and adding utc offset, then appending tzinfo) and finally strips the timezone.
    Converts string to naive pendulum datetime, stripping any timezone info
    >>> string_to_naive_utc0_datetime("2022-05-05T05:05:05+01:00").isoformat()
    '2022-05-05T04:05:05'
    >>> string_to_naive_utc0_datetime("2022-05-05").isoformat()
    '2022-05-05T00:00:00'
    >>> string_to_naive_utc0_datetime("1900-01-01").isoformat()
    '1900-01-01T00:00:00'
    """
    pdl_dt = pendulum.parser.parse(string)
    assert isinstance(pdl_dt, PendulumDateTime)
    pdl_dt = pdl_dt.in_timezone("UTC")
    pdl_dt = pdl_dt.naive()
    return pdl_dt


def epoch_to_naive_utc0_datetime(
    epoch: int | float
) -> datetime:
    """
    Parses integer/float to pendulum datetime, representing seconds since unix epoch, and strips the default UTC timezone.
    When converting from milliseconds, divide by 1000 first.

    example: forgetting to divide by 1000
    >>> try: epoch_to_naive_utc0_datetime(1706000773111)
    ... except ValueError as exc: exc.args[0]
    'year 56031 is out of range'

    example: remembering to do it
    >>> epoch_to_naive_utc0_datetime(1706000773111 / 1000).isoformat()
    '2024-01-23T09:06:13.111000'
    """
    pdl_dt = pendulum.from_timestamp(epoch) # default timezone UTC
    assert isinstance(pdl_dt, PendulumDateTime)
    pdl_dt = pdl_dt.naive()
    return pdl_dt


def datetime_to_naive_norwegian_datetime(dt: datetime) -> datetime:
    """converts datetime to naive norwegian datetime
    >>> datetime_to_naive_norwegian_datetime(datetime.fromisoformat("2022-05-05T05:05:05+01:00")).isoformat()
    '2022-05-05T06:05:05'
    """
    return string_to_naive_norwegian_datetime(dt.isoformat())


def naive_norwegian_datetime_to_naive_utc0_datetime(dt: datetime) -> datetime:
    """converts a naive norwegian datetime to a naive utc0 datetime
    >>> naive_norwegian_datetime_to_naive_utc0_datetime(datetime.fromisoformat("2023-08-29T21:16:48")).isoformat()
    '2023-08-29T19:16:48'
    """
    assert dt.tzinfo is None
    pdl_dt = PendulumDateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond)
    pdl_dt = pdl_dt.in_timezone("Europe/Oslo").in_timezone("UTC").naive()
    return pdl_dt


def string_to_code(string: str) -> str:
    """converts a string to a code string conforming to dwh standard
    >>> string_to_code("/&$  ØrkEn Rotte# *;-")
    'ORKEN_ROTTE'
    >>> string_to_code(" ??? ")
    'UKJENT'
    """
    code = string.upper().replace("Æ", "A").replace("Ø", "O").replace("Å", "AA")
    code = "_".join(
        word
        for word in re.findall(
            r"(\w*)",
            code,
        )
        if word
    )
    code = "UKJENT" if code == "" else code
    return code


def string_to_json(string: str) -> Any:
    """returns json object from string
    >>> string_to_json('{"x": 1}')
    {'x': 1}
    """
    return json.loads(string)


def json_to_string(data: Any) -> str:
    """returns json-serialized object (string)
    >>> json_to_string({"x": 1})
    '{"x": 1}'
    """
    return json.dumps(data, ensure_ascii=False)


def bytes_to_string(data: bytes) -> str:
    """returns the utf-8 decoded string
    >>> bytes_to_string(b'hello world')
    'hello world'
    """
    string = data.decode("utf-8")
    return string


def string_to_bytes(string: str) -> bytes:
    """returns the utf-8 encoded string as bytes
    >>> string_to_bytes('Hello, world!')
    b'Hello, world!'
    """
    data = string.encode("utf-8")
    return data


def json_bytes_to_string(data: bytes) -> str:
    """Returns json serialized object (string)
    >>> json_bytes_to_string(b'{"x": 35}')
    '{"x": 35}'
    """
    string = json.dumps(json.loads(data), ensure_ascii=False)
    return string


def bytes_to_sha256_hash(data: bytes) -> str:
    """Returns the sha256 hash of the bytes as a hex-numerical string
    >>> bytes_to_sha256_hash(b'Hello, world!')
    '315f5bdb76d078c43b8ac0064e4a0164612b1fce77c869345bfc94c75894edd3'
    """
    sha = hashlib.sha256(data).hexdigest()
    return sha


def string_to_sha256_hash(string: str) -> str:
    """Returns the sha256 hash of the utf-8 encoded string as a hex-numerical string
    >>> string_to_sha256_hash("Hello, world!")
    '315f5bdb76d078c43b8ac0064e4a0164612b1fce77c869345bfc94c75894edd3'
    """
    sha = hashlib.sha256(string_to_bytes(string)).hexdigest()
    return sha
