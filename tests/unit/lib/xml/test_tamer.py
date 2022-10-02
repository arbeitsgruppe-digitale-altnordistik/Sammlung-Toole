from pathlib import Path

import pytest
from lxml import etree as et
from src.lib.xml import tamer

test_data = Path("tests/testdata")


def test_load_xml_contents() -> None:
    xml_path = test_data / 'valid.xml'
    xml = tamer._load_xml_contents(xml_path)
    assert isinstance(xml, et._Element)
    assert et.tostring(xml)
    assert xml.tag == "xml"
    assert len(xml) == 4


def test_load_xml_contents_failing() -> None:
    res = tamer._load_xml_contents(test_data / 'invalid.xml')
    assert res is None
    res = tamer._load_xml_contents(Path('non-existing.xml'))
    assert res is None


@pytest.mark.skip(reason="has to be done, but not a priority right now")
def test_parse_xml_content() -> None:
    # TODO-BL: test this eventually... needs good test data!
    pass
