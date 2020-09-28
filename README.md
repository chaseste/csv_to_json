# csv_to_json
CSV to JSON transform

![coverage](https://img.shields.io/badge/coverage-93%25-brightgreen)
![python](https://img.shields.io/badge/python-3.8-blue)

##### Table of Contents
- [Motivation?](#motivation)
- [Why Bother?](#why-bother)
- [Design?](#design)
- [CSV](#csv)
  * [Delimiters](#delimiters)
  * [!Disclaimer!](#disclaimer)
  * [Sample](#sample)
- [JSON](#json)
  * [Raw (Unformatted)](#raw-unformatted)
  * [Formatted](#formatted)
- [Transforms](#transforms)
  * [Identity Transform](#identity-transform)
  * [Combine Transform](#combine-transform)
  * [Transform.py](#transformpy)
    + [Command](#command)
  * [Cython (Optional)](#cython-optional)
    + [Requirements](#requirements)
    + [Steps](#steps)
  * [Profiling](#profiling)
    + [Test Rig](#test-rig)
    + [Results](#results)
      - [Pure Python](#pure-python)
      - [Cython (Compiled Python)](#cython-compiled-python)
        * [profile=True](#profile-true)
        * [profile=False](#profile-false)
- [What's Left?](#whats-left)
- [Running the Tests](#running-the-tests)
  * [Source Code](#source-code)
  * [Installed Module](#installed-module)
- [Code Coverage](#code-coverage)
  * [Coverage Report](#report)

# Motivation
Python is considered a slow programming language. So much so that Python's performance is often ridiculed as its biggest weekness / downfall. My curiosity was to take a task thats split between IO and CPU bound to see how Python would fair. Would Python fair well? Would compiling to C make a significant difference? The task I picked was transforming a CSV specification from file to JSON. The specification is a good example since it doesn't work with Python's native parser.

# Why Bother?
CSV works great for simple data where the data is relatively flat (table dump, etc.) though for complex data where fields (columns) can repeat, have subfields, etc. CSV breaks down. The transformation of CSV into something more processable is mainly on each comsumer. Enter JSON. JSON works well with simple and complex data. Is supported by most if not all programming languages either natively or via extension. Is relatively light wieght and parsing is generally pretty fast since it powers the web. 

So why not update your process to output JSON and abandon CSV? Thats the hard part. Moving away from a standard encures a cost to consumers that have already figured out your CSV. Maintaining support for mutliple formats can add development | complexity | maintanence costs. Maybe your long term goal is to move away from CSV though doing so isn't feasible within your current tooling | infrastructure. Enter post processing. Post processing your existing CSV to an intermediate can be a reasonable way to leave your existing platform alone while providing data in a richer format that can be transformed | consumed more readily. Maybe you're implementing a data pipeline or importing the CSV into a system and would like to convert the CSV into format more readily consumed. Either way, JSON is a good answer.

So why Python? Python is a simple programming language to learn, has tons of libraries, community support, along with a robust job orchestraton / workflow platform like [Airflow](https://airflow.apache.org/). The biggest motivation in my opinion. Time. I'm by no means an expert Python programmer. In total I spent around 8 hours coding this example. Did I write tests? Not completely... though it wouldn't take much time to complete. Adding additional transforms is pretty straight forward. Adding the problem transform took around 30 minutes. Most of my time was spent creating the initial allergy transform and profiling the code.

Final comments. Whether you chose to use Python or not. Implementing JSON is beneficial for cases where consumers (clients) request (pay) for one off mappings to their proprietory spec. Given JSON is easy to work with, writing these one off transforms from JSON will reduce coding time. Using a program language like Python along with a worflow platform like airflow you can create transformation pipelines where the initial step is converting to JSON. JSON also allows for consolidation | grouping of data where the JSON object could echo your CSV where each row has a coresponding JSON object or the object contains similar rows (aka all person allergies vs a single allergy) without having to define a new object. 

So is Python good enough? Maybe...

# Design
The design is based on dispatch functions where the user / consumer specifies the transformation and its mapped to a transformer.

```
transform.py
    ...
    combine = "-C" == sys.argv[4].upper() if len(sys.argv) == 5 else False
    dispatch = { 
        "ALLERGY": transformers.AllergyToJson(combine=combine),
        "PROBLEM": transformers.ProblemToJson(combine=combine)
    }
	...
    src_dir, dest_dir = sys.argv[2], sys.argv[3]
    join, remove, transformer = os.path.join, os.remove, dispatch[trans_type]
    for filename in list(filter(lambda f: f.endswith(".csv"), os.listdir(src_dir))):
        src, dest = join(src_dir, filename), join(dest_dir, "".join([filename[:-3], "json"]))
        with open(src) as f_csv, open(dest, "w") as f_json:
            transformer.csv_to_json(f_csv, f_json)
        remove(src)
	...
```

Python supports OOP (Object Oriented Programming) though accessing methods from a class hierachy is slow. To give Python a chance a mix of OOP is used to define the transforms though common methods called frequently to perform the transform are left outside the class hierarchy. References to functions called frequently are used when possible to further aide / limit the amout of time spent by the interpreter. 

Finally references to functions are used to switch the transformation performed at runtime without having to decorate the class hierarchy. Doing so limits the performance impact of having multiple transforms (identity vs combining like instances) by having a single if statement evaluation when the transformer is constructed. This follows the idiom to try and fail instead of checking beforehand since the verification for each valid invocation is incurred. This idiom is applied as well where applicable.

```
csv_transfomer.py
""" CSV Transformation Base """
...
def identity_transform(orig: Dict[str, str], org_key: str, dest: Dict) -> None:
    """ Identity transform from one field in a dict to another """
    try:
        dest[org_key] = orig[org_key]
    except KeyError:
        pass
...
class CsvToJson(metaclass=ABCMeta):
    """ Base CSV to JSON transformer """

    def __init__(self, combine:bool = False):
        self.transformation = self.__combine_transform if combine else self.__idenity_transform

    def __idenity_transform(self, r):
        """ Performs an identity transformation where each row in the CSV will have a
            corresponding JSON record """
		...

    def __combine_transform(self, r):
        """ Performs an combine transformation where similar CSV records will be combined
            into a single JSON record """
		...

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
```

# CSV 
The CSV specification used is based on the Cerner Millennium extract specifications.

## Delimiters
| Specification | Value |
| --- | --- |
| Field Delimiter | , |
| Sub-Field Delimiter | \| |
| Repeat Delimiter | ~ |
| Text Qualifier | " |

## !Disclaimer!
The out of the box Python parser will only escape / maintain the escape "quotes" when the field delimiter is encapsulated within. To allow the escape quotes to escape the sub fields, a pure Python parser is needed to maintain the quotes while parsing the fields before removing them. Post processing the subfields is not possible since the quotes would be removed...

## Sample
```
1,12345|Hospital MRN|MRN~67890|HIE MRN|CMRN,ZZLast|Jane|Marie,19500701143000,362|Female,893727|Hospital FIN|FIN,Drug,723|Amoxicillin|RXCUI,SEVERE,20180724,CANCELED,CLASS,PARENT,Parent,20190813165421,20190813165431,498834018|"Abdominal swelling |~ distended areas"|SNOMED|777777|desc|SNOMED,13243|ZZPhylast|Robert|REV|NPI,"Discovered during ER visit ~July|August 2018"|06/19/1999 12:34:56|ID1234|Test1|Physician1|NPI~"Bad swelling to chest, head"|01/01/1991 01:11:11|ID5678|Test2|Physician2|NPI
```

# JSON

## Raw (Unformatted)
```
{"patient": {"ids": [{"id": "12345", "authority": "Hospital MRN", "id_type": "MRN"}, {"id": "67890", "authority": "HIE MRN", "id_type": "CMRN"}], "name": {"last": "ZZLast", "first": "Jane", "middle": "Marie"}, "birth_date": "19500701143000", "admin_sex": {"id": "362", "description": "Female"}}, "encounter": {"ids": [{"id": "893727", "authority": "Hospital FIN", "id_type": "FIN"}]}, "allergys": [{"allergen_type": {"id": "Drug"}, "allergen": {"id": "723", "description": "Amoxicillin", "coding_method": "RXCUI"}, "severity": {"id": "SEVERE"}, "onset": "20180724", "reaction_status": {"id": "CANCELED"}, "reaction_class": {"id": "CLASS"}, "source_of_info": {"id": "PARENT"}, "source_of_info_ft": "Parent", "cancel_dt_tm": "20190813165421", "reviewed_dt_tm": "20190813165431", "reactions": [{"code": {"id": "498834018", "description": "Abdominal swelling |~ distended areas", "coding_method": "SNOMED"}, "severity": {"id": "777777", "description": "desc", "coding_method": "SNOMED"}}], "physician": {"id": {"id": "13243", "authority": "NPI"}, "name": {"last": "ZZPhylast", "first": "Robert"}, "phys_type": "REV"}, "comments": [{"text": "Discovered during ER visit ~July|August 2018", "comment_dt_tm": "06/19/1999 12:34:56", "physician": {"id": {"id": "ID1234", "authority": "NPI"}, "name": {"last": "Test1", "first": "Physician1"}}}, {"text": "Bad swelling to \"chest\", head", "comment_dt_tm": "01/01/1991 01:11:11", "physician": {"id": {"id": "ID5678", "authority": "NPI"}, "name": {"last": "Test2", "first": "Physician2"}}}]}]}
```

## Formatted
```
{
	"patient": {
		"ids": [
			{
				"id": "12345",
				"authority": "Hospital MRN",
				"id_type": "MRN"
			},
			{
				"id": "67890",
				"authority": "HIE MRN",
				"id_type": "CMRN"
			}
		],
		"name": {
			"last": "ZZLast",
			"first": "Jane",
			"middle": "Marie"
		},
		"birth_date": "19500701143000",
		"admin_sex": {
			"id": "362",
			"description": "Female"
		}
	},
	"encounter": {
		"ids": [
			{
				"id": "893727",
				"authority": "Hospital FIN",
				"id_type": "FIN"
			}
		]
	},
	"allergys": [
		{
			"allergen_type": {
				"id": "Drug"
			},
			"allergen": {
				"id": "723",
				"description": "Amoxicillin",
				"coding_method": "RXCUI"
			},
			"severity": {
				"id": "SEVERE"
			},
			"onset": "20180724",
			"reaction_status": {
				"id": "CANCELED"
			},
			"reaction_class": {
				"id": "CLASS"
			},
			"source_of_info": {
				"id": "PARENT"
			},
			"source_of_info_ft": "Parent",
			"cancel_dt_tm": "20190813165421",
			"reviewed_dt_tm": "20190813165431",
			"reactions": [
				{
					"code": {
						"id": "498834018",
						"description": "Abdominal swelling |~ distended areas",
						"coding_method": "SNOMED"
					},
					"severity": {
						"id": "777777",
						"description": "desc",
						"coding_method": "SNOMED"
					}
				}
			],
			"physician": {
				"id": {
					"id": "13243",
					"authority": "NPI"
				},
				"name": {
					"last": "ZZPhylast",
					"first": "Robert"
				},
				"phys_type": "REV"
			},
			"comments": [
				{
					"text": "Discovered during ER visit ~July|August 2018",
					"comment_dt_tm": "06/19/1999 12:34:56",
					"physician": {
						"id": {
							"id": "ID1234",
							"authority": "NPI"
						},
						"name": {
							"last": "Test1",
							"first": "Physician1"
						}
					}
				},
				{
					"text": "Bad swelling to \"chest\", head",
					"comment_dt_tm": "01/01/1991 01:11:11",
					"physician": {
						"id": {
							"id": "ID5678",
							"authority": "NPI"
						},
						"name": {
							"last": "Test2",
							"first": "Physician2"
						}
					}
				}
			]
		}
	]
}
```

# Transforms
As of now only Allergy | Problem transforms were written.

## Identity Transform
Each record in the CSV will have a corresponding JSON record in the output file. This is the default.

## Combine Transform
Records for the same person in the CSV will be combined into a single JSON record in the output file. A person is considered the same when they have the same name, birth date and gender.

## Transform.py
The transformation script takes in four arguments. The type of transformation, the "in" directory to scan and the "out" directory to write the json files to are required. The option (-c or -C) to combine records for a person is optional. The output file will maintain the original csv file name. Upon completion the csv file will be deleted from the "in" directory.

```
python transform.py type in_dir out_dir option
```

## Cython (Optional)
Python to slow? Enter Cython. Python's csv reader is written in C. Cython can be used to improve the performance of your transforms | csv reader without having to change one line of Python code. 

### Requirements
Windows is covered here. Linux / Unix should be fairly straight forward.

- [Cython](https://pypi.org/project/Cython/)
- [Build Tools for Visual Studio 2019](https://visualstudio.microsoft.com/thank-you-downloading-visual-studio/?sku=BuildTools&rel=16)

### Steps
- Install Cython
```
pip install cython
```
- Download the VS build tools. See [Python compilers](https://wiki.Python.org/moin/WindowsCompilers)
- Build the transforms
```
python setup.py build_ext --inplace
```

## Profiling
I profiled the allergy transform to get an idea of how "bad" or "good" an idea using Python is. Data was collected using cProfile with a 488 MB CSV containing a little over 1M rows. The CSV was constructed from the sample allergy csv. The default transform was used.

### Command
```
python -m cProfile transform.py ALLERGY in_dir out_dir > log.txt
```

### Test Rig
Profiling was run on a Lenovo w541 with an Intel i7-4940MX CPU @3.10GHz, 4 Core(s), 8 Logical Processor(s) along with 32GB of ram and a 512GB SSD. Applications (Edge, Chrome, VScode, etc.) were running on the machine to simulate additional load. Around 10GB of physical ram was in use along with ~20% CPU usage (mostly by Python). The rig is by no means a race horse...

### Results
The results definitly speak for themselves. Running the source "as is" without Cython shows how pure Python performs without C extensions. Cython produced a ~65% reduction in time (profiling csv_reader disabled) with a ~50% reduction when enabled. A furthur reduction could be seen by optimizing the Python scripts (defining types, etc.) to help Cython generate the code. This optimization was not implemented to keep the source pure while showing how simply converting slower sections of code to C can drastically improve the performance without sacrificing the simplicity of Python. 

Are these results remarkable? Sure. Pure Python netted ~2420 tps (transforms per second) while the Cython produced a best of ~7004 tps. Is this earth shattering, no. Does this show that while Python isn't considered fast, it might be fast enough for your use cases? Sure. 

#### Pure Python
| ncalls | cumtime | filename:lineno(function) |
| --- | --- | --- |
| 1 | 439.062 | transform.py:7(main) |
| 1062501 | 307.131 | csv_reader.py:65(__next__) |
| 20187500 | 229.482 | csv_reader.py:19(parse_fields) |
| 140250000 | 191.839 | csv_reader.py:7(split_escaped) |

#### Cython (Compiled Python)
By default Cython won't provide profiling information. To enable this for csv_reader.py, change profile from False to True. Make sure to rebuild the source and clear the py_cache.
```
# cython: profile=True
```

##### profile=True
| ncalls | cumtime | filename:lineno(function) |
| --- | --- | --- |
| 1 | 227.614 | transform.py:7(main) |
| 1062501 | 139.484 | csv_reader.py:65(__next__) |
| 20187500 | 103.703 | csv_reader.py:19(parse_fields) |
| 140250000 | 64.585 | csv_reader.py:7(split_escaped) |

##### profile=False
| ncalls | cumtime | filename:lineno(function) |
| --- | --- | --- |
| 1 | 151.697 | transform.py:7(main) |

# What's Left?
- Harden parser
- Additional unit tests
- Additional combine logic?
- Additional transforms?
- Profile combine transform?

# Running the Tests
The tests can be run locally against the source code or the installed module.

## Source Code
When running the tests locally make sure the module has been uninstalled (if previously installed) along with deleting the __pycache__ in csv_to_json to ensure the scripts are used.

```
# Ensure the module has been uninstalled (ommit if not installed)
pip uninstall csv_to_json
# Ensure pytest can find your module when it hasn't been installed
set PYTHONPATH=.
pytest
```

## Installed Module
```
# Ensure pytest is using the installed module (ommit if not previously set)
set PYTHONPATH=
pip install .
pytest
```

# Code Coverage
Code coverage is provided by [pytest-cov](https://github.com/pytest-dev/pytest-cov)

```
pytest --cov=csv_to_json tests/
```

## Coverage Report
```
----------- coverage: platform win32, python 3.8.5-final-0 -----------
Name                            Stmts   Miss  Cover
---------------------------------------------------
csv_to_json\__init__.py             0      0   100%
csv_to_json\csv_reader.py          52      3    94%
csv_to_json\csv_transfomer.py     105      9    91%
csv_to_json\transformers.py        56      2    96%
---------------------------------------------------
TOTAL                             213     14    93%

```
