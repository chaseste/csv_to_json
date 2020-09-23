from csv_to_json.csv_transfomer import transform_fields, identity_transform

def test_identity_transform():
    orig, trans = { "FIELD1": "VAL1" }, {}
    identity_transform(orig, "FIELD1", trans)
    assert trans == orig

def test_transform_fields():
    expected, trans = { "FIELD3": { "FIELD2": "VAL1" } }, {}
    transform_fields({ "FIELD1": "VAL1" }, { "FIELD1": "FIELD2", "FIELD4": "FIELD5" }, trans, "FIELD3")
    assert trans == expected

def test_transform_fields_empty():
    expected, trans = {}, {}
    transform_fields({}, { "FIELD1": "FIELD2" }, trans, "FIELD3")
    assert trans == expected
