""" CSV Reader """
# cython: profile=False

from typing import Iterator, List, Dict, TextIO
from abc import ABCMeta, abstractmethod

class CsvReader(metaclass=ABCMeta):
    """ A simple CSV parser specific to our delimiters 
        
        Delimiters:
        Field: ,
        Subfield: |
        Escape: "
        Repeat: ~ """

    def __init__(self, f: TextIO, field_cnt: int):
        self.f = f
        self.field_cnt = field_cnt 
    
    def __iter__(self):
        return self

    @abstractmethod
    def __next__(self):
        raise StopIteration()

def strip_quotes(data:str) -> str:
    """ Removes escape quotes from the data """
    return data[1:-1].replace("\"\"", "\"") if data.startswith("\"") and data.endswith("\"") else data

def split_escaped(data:str, delim:str) -> Iterator[str]:
    """ Splits the data using the specified delimiter accounting for 
        double quotes to escape the delimiter """
    pos, escape = 0, False
    for i, c in enumerate(data):
        if c == "\"":
            escape = not escape
        elif c == delim and not escape:
            yield data[pos: i]
            pos = i + 1
    yield data[pos:]

def parse_fields(field:str) -> List[List[str]]:
    """ Parses the fields from the csv field accounting for repeating fields along with subfields """
    return list(list(map(strip_quotes, split_escaped(i, "|"))) for i in split_escaped(field, "~") if i)

def csv_to_dict(data: List[List], fields: List[str]) -> Dict[str, str]:
    """ Converts the raw csv column to a dict with the specified field names """
    try:
        return dict(zip(fields, data[0]))
    except IndexError:
        return {}

def csv_to_list(data: List[List], fields: List[str]) -> List[Dict[str, str]]:
    """ Converts the raw csv column to a list of dicts with the specified field names """
    return list(dict(zip(fields, f)) for f in data if f)

def reader(f: TextIO, field_cnt: int) -> CsvReader:
    """ Replaces the native CSV reader since it strips quotes when the field 
    doesn't contain a comma... Disabling quoting breaks when the field does 
    contain a comma but fixes when the sub delimiters are present. 
    
        The header will be skipped if present in the CSV file. Looking for 
    the SEQ header should be sufficient to skip
    """
    inst = _CsvReaderImpl(f, field_cnt)

    pos = f.tell()
    has_header = f.read(4).upper().startswith("SEQ|")
    if has_header:
       f.readline()
    else:
        f.seek(pos)
    return inst

class _CsvReaderImpl(CsvReader):    

    def __next__(self):
        line = self.f.readline()
        if not line:
            raise StopIteration()

        if line.endswith("\n\r"):
            line = line[:-2]
        elif line.endswith("\n") or line.endswith("\r"):
            line = line[:-1]
        
        row = list(map(parse_fields, split_escaped(line, ",")))
        for _ in range(len(row), self.field_cnt):
            row.append([])
        return row
