# cython: profile=False

def strip_quotes(data:str) -> str:
    """ Removes starting / ending quotes (escape) from the data """
    return data[1:-1] if data.startswith("\"") and data.endswith("\"") else data

def split_escaped(data:str, delim:str):
    """ Splits the data using the specified delimiter accounting for 
        double quotes to escape the delimiter """
    pos, escape = 0, False
    for i, c in enumerate(data):
        if c == "\"":
            escape = not escape
        elif c == delim and not escape:
            yield data[pos: i]
            pos = i+1
    yield data[pos:]

def parse_fields(field:str) -> list:
    """ Parses the fields from the csv field accounting for repeating fields along with subfields """
    return list(list(map(strip_quotes, split_escaped(i, "|"))) for i in split_escaped(field, "~") if i)

def csv_to_dict(data, fields):
    """ Converts the raw csv column to a dict with the specified field names """
    try:
        return dict(zip(fields, data[0]))
    except IndexError:
        return {}

def csv_to_list(data, fields):
    """ Converts the raw csv column to a list of dicts with the specified field names """
    return list(dict(zip(fields, f)) for f in data)

def reader(f, field_cnt):
    """ Override the CSV's native readersince it strips quotes when the field 
    doesn't contain a comma... Disabling quoting breaks when the field does 
    contain a comma but fixes when the sub delimiters are present instead.
    Unfortunately the only solution is to substitute the reader while maintaining
    the dict reader's functionality """
    inst = CsvReader(f, field_cnt)

    """ Look to see if the file contains a header or not, if so, we'll skip it, otherwise 
        we'll leave the file alone. Looking for the SEQ header should be sufficient to skip """
    pos = f.tell()
    has_header = f.read(4).upper().startswith("SEQ|")
    if has_header:
       f.readline()
    else:
        f.seek(pos)
    return inst

class CsvReader:
    """ A simple CSV parser that needs to be extended for robustness.
        Python removes quotes if the line doesn't contain the delimiter
        which doesn't work for this application since the CSV has delimiters
        for subfields that need the quotes to escape them. """

    def __init__(self, f, field_cnt):
        self.f = f
        self.field_cnt = field_cnt 
    
    def __iter__(self):
        return self
    
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
