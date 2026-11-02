from dwh_oppfolging.apis.brreg_api_v2 import BrregUnitAPI as api
from dwh_oppfolging.apis.brreg_api_v2 import string_to_naive_norwegian_datetime
from datetime import datetime

def test_get_unit():
    raise NotImplementedError
    enhet_api = api('Enhet', datetime.now())

    orgnr = [
        925280119
    ]

    for k, v in enhet_api.get_unit_as_row(str(orgnr[0])).items():
        print(k)
        print(v)
    # oppdatert_tid_kilde
    # 2026-01-27 09:40:11.811000
    #
    updates = enhet_api.get_unit_update_history(str(orgnr[0]))
    for update in updates:
        update["dato"] = string_to_naive_norwegian_datetime(update["dato"]).isoformat()
    updates.sort(key=lambda x: x["dato"])
    for update in updates:
        print(update)