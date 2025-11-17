import pytest

pytestmark = pytest.mark.local

from dwh_oppfolging.apis.ssb_api_v1_pydantic import Classification, Version, CorrespondenceTable

def test_version_from_classification():
    c = Classification(2)
    v = Version(c.versions[0].ID)

def test_correspondence_table_from_version():
    v = Version(403)
    cr = CorrespondenceTable(v.correspondence_tables[0].ID)

def test_version_with_variant():
    v = Version(916)
    assert v.classification_variants