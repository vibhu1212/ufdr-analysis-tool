import pytest
from database.schema import extract_phone_suffix

def test_extract_phone_suffix_happy_path():
    assert extract_phone_suffix("1234567890", 2) == "90"
    assert extract_phone_suffix("1234567890", 4) == "7890"

def test_extract_phone_suffix_edge_cases():
    assert extract_phone_suffix("", 2) == ""
    assert extract_phone_suffix(None, 2) == ""
    assert extract_phone_suffix("123", 4) == ""
