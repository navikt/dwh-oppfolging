"Sub-module for data types"

from dataclasses import dataclass
from collections.abc import MutableMapping, Sequence
from functools import reduce
from typing import Callable, Any, Type, Mapping
from datetime import datetime
from yaml import safe_load
from dwh_oppfolging.transforms.functions import (
    find_in_dict,
    string_to_naive_norwegian_datetime,
    epoch_to_naive_utc0_datetime,
    string_to_code,
    datetime_to_naive_norwegian_datetime
)


# a dict but covariant in values and setitem defined (i.e. functions are allowed to change records)
# values limited to subset of the supported values in oracledb
Record = MutableMapping[str, str | int | float | bytes | datetime | None]

# a lightweight immutable version of Record with only positional fields
FixedRecord = Sequence[str | int | float | bytes | datetime | None]


class StopWatch():
    """simple start-stop timer, call it to get new lap time"""
    def __init__(self) -> None:
        self.start = datetime.today()
    def __call__(self):
        start, end = self.start, datetime.today()
        self.start = end
        return end - start


@dataclass
class FieldSchema:
    """
    A highly customizable ruleset for the extraction of a single datafield from a mapping
    with string keys representing a record.
    Use together with RecordSchema to extract collections of fields.

    params:
        - src: str
            the desired key-path, delimited by "." (by default) to get nested keys
            i.e. src="k1.k2" -> data["k1"]["k2"]
            NB: this is the only mandatory parameter.
        - dtype (optional, default None): type
            the expected type of the value at src
            if set, a TypeError is raised if the data at src is not of this type
        - dst (optional, defaults to src in post-init): str
            the destination key, only relevant if needing to construct a record like {dst: value}
            NB: the dst key is never interpreted as a path
        - allow_undefined (optional, default False):
            if set, missing key-paths will not raise a KeyError
        - allow_none (optional, default False): bool
            if set, None type data will not raise a ValueError
        - replace_undefined_with (optional, default None): Any
            the default value to return if the key-path was not found in the data
            only relevant if allow_undefined is set
            NB: if this is set to a not-None value, allow_undefined is set to True in post-init.
        - replace_none_with (optional, default None): Any
            the value replacing None if None was found at the key-path
            NB: if this is set to a not-None value, allow_none is overriden to True in post-init.
        - transform (optional, default None): callable
            if set, transform is called on the value found at key-path
        - transform_replacements (optional, default False): bool
            if set, transform is also called on replaced values, for both none and undefined.
            NB: if set, but transform is not set, a TypeError is raised in post-init.
        - delimiter (optional, default '.'): str
            the delimiter to use when interpreting src string as a key-path.
    """

    src: str
    dtype: Type|None = None
    dst: str|None = None
    allow_undefined: bool = False
    allow_none: bool = False
    replace_undefined_with: Any = None
    replace_none_with: Any = None
    transform: Callable|None = None
    transform_replacements: bool = False
    delimiter: str = "."

    def __post_init__(self):
        if self.dst is None:
            self.dst = self.src
        if self.transform_replacements and self.transform is None:
            raise TypeError("transform function is not set even though transform_replacements is set")
        if self.replace_undefined_with is not None:
            self.allow_undefined = True
        if self.replace_none_with is not None:
            self.allow_none = True

    def _get_value(self, data: Mapping) -> Any:
        try:
            value: Any = reduce(lambda d,k: d[k], self.src.split(self.delimiter), data)
        except (KeyError, TypeError, IndexError): # if other errors occur they are outside object.__getitem__ spec
            if not self.allow_undefined:
                raise KeyError(f"{self.src} is not allowed to be undefined")
            elif self.transform_replacements: # if transform is None here, expect raised error
                return self.transform(self.replace_none_with) # type: ignore
            else:
                return self.replace_undefined_with
        else:
            if value is None:
                if not self.allow_none:
                    raise ValueError(f"{self.src} is not allowed to be None")
                elif self.transform_replacements: # if transform is None here, expect raised error
                    return self.transform(self.replace_none_with) # type: ignore
                else:
                    return self.replace_none_with
            else:
                if self.dtype is not None and not isinstance(value, self.dtype):
                    raise TypeError(f"{self.src} is not {self.dtype} but {type(value)}")
                if self.transform is not None:
                    return self.transform(value)
                else:
                    return value

    def __call__(self, data: Mapping, as_record: bool = False) -> Any:
        """
        Extracts the field's value from the data.

        params:
            - data: dict or dict-like
            - as_record (optional, default False): bool
                if set, returns a dict instead of value

        returns:
            value or {dst or src: value} if as_record

        >>> FieldSchema('x', int)({'x': 1})
        1
        >>> try: FieldSchema('x', int)({'x': 0.882})
        ... except TypeError: "not int!"
        'not int!'
        >>> try: FieldSchema('x')({})
        ... except KeyError: "no x!"
        'no x!'
        >>> try: FieldSchema('x')({'x': None})
        ... except ValueError: "empty x!"
        'empty x!'
        >>> FieldSchema('x', allow_undefined=True)({}) is None
        True
        >>> FieldSchema('x', dst='y', replace_none_with=-1)({'x': None}, as_record=True)
        {'y': -1}
        >>> FieldSchema('x', float, 'int_x', True, transform=lambda x: int(x))({'x': 1.5})
        1
        """
        value = self._get_value(data)
        if as_record:
            return {self.dst: value}
        else:
            return value

@dataclass
class RecordSchema:
    """
    A schema for extracting a record of datafields; a collection of FieldSchemas.

    params:
        - fields, a list of FieldSchemas
    """

    fields: list[FieldSchema]

    def __call__(self, data: Mapping):
        """
        extracts record from dict
        >>> RecordSchema([FieldSchema('x', int, 'y')])({'x': 1})
        {'y': 1}
        """
        return {field.dst: field(data) for field in self.fields}

    def get_field_names(self, renamed_names: bool = False):
        """
        returns field names, optionally the renamed ones
        >>> RecordSchema([FieldSchema('x', int, 'y')]).get_field_names(True)
        ['y']
        """
        return [field.dst if renamed_names else field.src for field in self.fields]

    @classmethod
    def from_yaml(cls, data: str):
        """
        Creates a RecordSchema from a yaml document string. 
        It is expected that the yaml document has key fields which contains a list
        of mappings with key:value pairs matching the FieldSchema init signature.
        The available transform functions from the yaml document are:
            - "str -> str-code"
            - "str -> datetime-no"
            - "int-unix-s -> datetime-no"
            - "int-unix-ms -> datetime-no"
            - "bool -> int"
            - "datetime -> datetime-no"

        params:
            - data: yaml string

        returns
            - RecordSchema

        >>> f = \"\"\"
        ... fields:
        ...     - src: myStruct.text
        ...       dst: code
        ...       transform: str -> str-code
        ...       replace_undefined_with: 'UNKNOWN'
        ...     - src: myId
        ...       dst: id
        ...       allow_undefined: true
        ... \"\"\"
        >>> rs = RecordSchema.from_yaml(f)
        >>> rs({'myStruct': {'text': 'hello, world!'}})
        {'code': 'HELLO_WORLD', 'id': None}
        >>> rs({'myId': 598782})
        {'code': 'UNKNOWN', 'id': 598782}
        """
        _YAML_FUN_LKP = {
            "str -> str-code": string_to_code,
            "str -> datetime-no": string_to_naive_norwegian_datetime,
            "int-unix-s -> datetime-no": epoch_to_naive_utc0_datetime,
            "int-unix-ms -> datetime-no": lambda x: epoch_to_naive_utc0_datetime(x/1000),
            "bool -> int": int,
            "datetime -> datetime-no": datetime_to_naive_norwegian_datetime,
        }
        document = safe_load(data)
        fields = []
        for rule in document["fields"]:
            if "transform" in rule:
                rule["transform"] = _YAML_FUN_LKP[rule["transform"]]
            fields.append(FieldSchema(**rule))
        return RecordSchema(fields)