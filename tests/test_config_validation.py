import xml.etree.ElementTree as ET
from pathlib import Path


def test_clickhouse_memory_xml():
    xml_path = (
        Path(__file__).resolve().parent.parent / "infra" / "clickhouse" / "config.d" / "memory.xml"
    )
    assert xml_path.exists(), f"File {xml_path} does not exist"

    tree = ET.parse(xml_path)
    root = tree.getroot()
    assert root.tag == "clickhouse"

    mem_usage = root.find("max_server_memory_usage")
    assert mem_usage is not None
    assert int(mem_usage.text) == 536870912  # 512 MB

    mark_cache = root.find("mark_cache_size")
    assert mark_cache is not None
    assert int(mark_cache.text) == 67108864  # 64 MB
