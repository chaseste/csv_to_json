import json

from abc import ABCMeta, abstractmethod
from .csv_reader import reader, csv_to_list, csv_to_dict

def identity_transform(orig, org_key, dest, dest_key=None):
    """ Identity transform from one field in a dict to another """
    try:
        dest[dest_key or org_key] = orig[org_key]
    except KeyError:
        pass
    
def transform_fields(orig, fields, dest, key):
    """ Transforms the original fields into a new dict with the specified new names """
    resp = dict((n, orig[k]) for (k, n) in fields.items() if k in orig)
    if resp:
        dest[key] = resp

def transform_string(data):
    """ Transforms the field to a string if it exists in the csv """
    try:
        return data[0][0]
    except (IndexError):
        return ""

def transform_string_optional(data, dest, key):
    """ Transforms the field to a string if it exists in the csv """
    trans = transform_string(data)
    if trans:
        dest[key] = trans

def transform_ids(data):
    """ Converts the raw csv ids to a list of JSON ids """
    return csv_to_list(data, ["id", "authority", "id_type"])

def transform_code(data, dest=None, key=None):
    """ Converts the raw csv code to a JSON code """
    return csv_to_dict(data, ["id", "description", "coding_method"])

def transform_code_optional(data, dest, key):
    """ Converts the raw csv code to a JSON code if the code exists in the csv """
    code = transform_code(data)
    if code:
        dest[key] = code

def transform_codes(data):
    """ Converts the raw csv codes to a list of JSON codes """
    return csv_to_list(data, ["id", "description", "coding_method"])

def transform_codes_optional(data, dest, key):
    """ Converts the raw csv codes to a list of JSON codes """
    codes = transform_codes(data)
    if codes:
        dest[key] = codes
    
def transform_name(data):
    """ Converts the raw csv name to a JSON name """
    return csv_to_dict(data, ["last", "first", "middle"])

def transform_physician(data, dest):
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

def transform_comments(data, dest):
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

def transform_reactions(data, dest):
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

    @abstractmethod
    def transform(self, fields):
        """ Entity / Domain specific transformation """ 
        return None

    def csv_to_json(self, csv_file, json_file):
        """ Converts the CSV to JSON """

        write = json_file.write
        transform = self.transform
        dumps = json.dumps

        r = reader(csv_file, getattr(self, "__fields__"))
        write(dumps(transform(next(r))))
        for fields in r:
            write("\n")
            write(dumps(transform(fields)))
