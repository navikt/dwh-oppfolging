"Datatypes used by oracle api"

from datetime import datetime
from typing import Sequence, Iterator


Row = dict[str, str | int | float | bytes | datetime | None]
BatchedRow = Sequence[Row]
GeneratedRow = Iterator[Row]
GeneratedBatchedRow = Iterator[Sequence[Row]]
BatchedBatchedRow = Sequence[Sequence[Row]]
