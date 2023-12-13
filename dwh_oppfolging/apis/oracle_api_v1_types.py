"Datatypes used by oracle api"

from datetime import datetime


Row = dict[str, str | int | float | bytes | datetime | None]
