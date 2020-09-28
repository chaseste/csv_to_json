""" CSV Transformation Base """
import json
from abc import ABCMeta, abstractmethod
from typing import Union, List, Dict, TextIO
from .csv_reader import reader, csv_to_list, csv_to_dict

def is_same_person(trans: Dict, next_trans: Dict) -> bool:
    """ Whether the person is the same for both transformations """
    patient = trans["patient"]
    next_patient = next_trans["patient"]
    return patient["name"] == next_patient["name"] and patient["birth_date"] == next_patient["birth_date"] and\
        patient["admin_sex"] == next_patient["admin_sex"]

def identity_transform(orig: Dict[str, str], org_key: str, dest: Dict) -> None:
    """ Identity transform from one field in a dict to another """
    try:
        dest[org_key] = orig[org_key]
    except KeyError:
        pass
    
def transform_fields(orig: Dict[str, str], fields: Dict[str, str], dest: Dict, key: str) -> None:
    """ Transforms the original fields into a new dict with the specified new names """
    resp = dict((n, orig[k]) for (k, n) in fields.items() if k in orig)
    if resp:
        dest[key] = resp

def transform_string(data: List[List]) -> str:
    """ Transforms the field to a string if it exists in the csv """
    try:
        return data[0][0]
    except (IndexError):
        return ""

def transform_string_optional(data: List[List], dest: Dict, key: str) -> None:
    """ Transforms the field to a string if it exists in the csv """
    trans = transform_string(data)
    if trans:
        dest[key] = trans

def transform_ids(data: List[List]) -> List[Dict[str, str]]:
    """ Converts the raw csv ids to a list of JSON ids """
    return csv_to_list(data, ["id", "authority", "id_type"])

def transform_code(data: List[List]) -> Dict[str, str]:
    """ Converts the raw csv code to a JSON code """
    return csv_to_dict(data, ["id", "description", "coding_method"])

def transform_code_optional(data: List[List], dest: Dict, key: str) -> None:
    """ Converts the raw csv code to a JSON code if the code exists in the csv """
    code = transform_code(data)
    if code:
        dest[key] = code

def transform_codes(data: List[List]) -> List[Dict[str, str]]:
    """ Converts the raw csv codes to a list of JSON codes """
    return csv_to_list(data, ["id", "description", "coding_method"])

def transform_codes_optional(data: List[List], dest: Dict, key: str) -> None:
    """ Converts the raw csv codes to a list of JSON codes """
    codes = transform_codes(data)
    if codes:
        dest[key] = codes
    
def transform_name(data: List[List]) -> Dict[str, str]:
    """ Converts the raw csv name to a JSON name """
    return csv_to_dict(data, ["last", "first", "middle"])

def transform_physician(data: Union[List[List], Dict], dest: Dict) -> None:
    """ Transforms the raw csv dict to a json physician dict """         
    try:
        data = csv_to_dict(data, ["phys_id", "phys_last", "phys_first", "phys_type", "phys_assign_auth"])
    except KeyError:
        pass

    physician = {}
    
    transform_fields(data, {
        "phys_id": "id",
        "phys_assign_auth": "authority", 
    }, physician, "id")
    transform_fields(data, {
        "phys_last": "last",
        "phys_first": "first"
    }, physician, "name")
    identity_transform(data, "phys_type", physician)
    
    if physician:
        dest["physician"] = physician

def transform_comments(data: List[List], dest: Dict) -> None:
    """ transforms the raw csv comments to a list of JSON comments """        
    comments = []
    append = comments.append
    for raw in csv_to_list(data, ["text", "comment_dt_tm", "phys_id", "phys_last", "phys_first", "phys_assign_auth"]):
        trans = {
            "text": raw["text"]
        }
        identity_transform(raw, "comment_dt_tm", trans)
        
        transform_physician(raw, trans)
        append(trans)
    if comments:
        dest["comments"] = comments

def transform_reactions(data: List[List], dest: Dict) -> None:
    reactions = []
    append = reactions.append
    for raw in csv_to_list(data, ["react_id", "react_descrip", "react_cod_method", "severity_id", "severity_descrip", "severity_cod_meth"]):
        reaction = {}

        transform_fields(raw, {
                "react_id": "id", 
                "react_descrip": "description", 
                "react_cod_method": "coding_method", 
        }, reaction, "code")
        transform_fields(raw, {
                "severity_id": "id",
                "severity_descrip": "description",
                "severity_cod_meth": "coding_method"
        }, reaction, "severity")
        
        if reaction:
            append(reaction)
    if reactions:
        dest["reactions"] = reactions

class CsvToJson(metaclass=ABCMeta):
    """ Base CSV to JSON transformer """

    def __init__(self, combine:bool = False):
        self.transformation = self.__combine_transform if combine else self.__idenity_transform

    def __idenity_transform(self, r):
        """ Performs an identity transformation where each row in the CSV will have a
            corresponding JSON record """
        transform = self.transform

        for fields in r:
            yield transform(fields)

    def __combine_transform(self, r):
        """ Performs an combine transformation where similar CSV records will be combined
            into a single JSON record """
        transform = self.transform
        combine = self.combine

        trans = transform(next(r))
        for fields in r:
            _next = transform(fields)
            if not combine(trans, _next):                    
                yield trans
                trans = _next
        yield trans

    @abstractmethod
    def transform(self, fields: List[List[str]]) -> Dict:
        """ Entity / Domain specific transformation """ 
        return {}

    @abstractmethod
    def combine(self, trans: Dict, next: Dict) -> bool:
        """ Entity / Domain specific combine """ 
        return False

    def csv_to_json(self, csv_file: TextIO, json_file: TextIO) -> None:
        """ Converts the CSV to JSON """
        write = json_file.write
        dumps = json.dumps

        r = reader(csv_file, getattr(self, "__fields__"))
        transform = self.transformation(r)

        write(dumps(next(transform)))
        for trans in transform:
            write("\n")
            write(dumps(trans))
