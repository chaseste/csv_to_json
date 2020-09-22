from csv_to_json.csv_reader import *

def test_strip_quotes_no_quotes():
    assert strip_quotes("abc") == "abc"

def test_strip_quotes():
    assert strip_quotes("\"abc\"") == "abc"

def test_strip_quotes_encapsulated():
    assert strip_quotes("\"a\"\"b\"\"c\"") == "a\"b\"c"

def test_split_escaped_no_delim():
    assert list(split_escaped("a", ",")) == ["a"]

def test_split_escaped():
    assert list(split_escaped("a,b,c", ",")) == ["a", "b", "c"]

def test_split_escaped_encapsulated():
    assert list(split_escaped("\"a,\"\"\",b,\",|~\"", ",")) == ["\"a,\"\"\"", "b", "\",|~\""]

def test_parse_fields_no_delim():
    assert parse_fields("a") == [["a"]]

def test_parse_fields():
    assert parse_fields("a|b|c") == [["a", "b", "c"]]

def test_parse_fields_encapsulated():
    assert parse_fields("\"a,\"\"\"|b|\",|~\"") == [["a,\"", "b", ",|~"]]

def test_parse_fields_repeat():
    assert parse_fields("a|b|c~d|e|f") == [["a", "b", "c"], ["d", "e", "f"]]

def test_csv_to_empty_dict():
    assert csv_to_dict([[]], ["FIELD1", "FIELD2"]) == {}

def test_csv_to_dict():
    assert csv_to_dict([["a", "b"]], ["FIELD1", "FIELD2"]) == { "FIELD1": "a", "FIELD2": "b" }

def test_csv_to_empty_list():
    assert csv_to_list([[]], ["FIELD1", "FIELD2"]) == []

def test_csv_to_list():
    assert csv_to_list([["a", "b"]], ["FIELD1", "FIELD2"]) == [{ "FIELD1": "a", "FIELD2": "b" }]

def test_reader():
    with open("tests/resources/test_csv_reader.csv") as csv:
        assert list(reader(csv, 3)) == [[[["1"]], [["a", "b", "c"], ["d", "e", "f"]], [["d"]], [["e", "f\""], ["a~", "b|", "c,"]]]]
