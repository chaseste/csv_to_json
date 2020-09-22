import pytest
import json

from typing import Iterator
from testfixtures import TempDirectory
from pytest_mock import MockerFixture

from csv_to_json.transformers import *

@pytest.fixture()
def dir() -> Iterator[TempDirectory]:
    with TempDirectory() as dir:
        yield dir

def test_allergy_transform(dir: TempDirectory, mocker: MockerFixture) -> None:
    """ Tests the allergy transform """

    with open("tests/resources/allergy.csv") as f_csv, open(dir.getpath("allergy.json"), "w") as f_json:        
        allergyToJson = AllergyToJson()
        spy = mocker.spy(allergyToJson, 'transform')

        allergyToJson.csv_to_json(f_csv, f_json)
        assert spy.spy_return
        assert len(spy.spy_return["allergys"]) == 1
        assert spy.spy_return["allergys"][0]["onset"] == "20180724"

    with open(dir.getpath("allergy.json")) as f_json:
        lines = f_json.readlines()
        assert len(lines) == 1
        
        _json = json.loads(lines[0])
        assert _json["birth_date"] == "19500701143000"
        assert _json["allergys"][0]["onset"] == "20180724"

def test_problem_transform(dir: TempDirectory, mocker: MockerFixture) -> None:
    """ Tests the problem transform """

    with open("tests/resources/problem.csv") as f_csv, open(dir.getpath("problem.json"), "w") as f_json:        
        problemToJson = ProblemToJson()
        spy = mocker.spy(problemToJson, 'transform')

        problemToJson.csv_to_json(f_csv, f_json)
        assert spy.spy_return
        assert spy.spy_return["birth_date"] == "19500701143000"
        assert len(spy.spy_return["problems"]) == 1
        assert spy.spy_return["problems"][0]["annotated_display"] == "This is the annotated display."

    with open(dir.getpath("problem.json")) as f_json:
        lines = f_json.readlines()
        assert len(lines) == 1

        _json = json.loads(lines[0])
        assert _json["birth_date"] == "19500701143000"
        assert _json["problems"][0]["annotated_display"] == "This is the annotated display."
