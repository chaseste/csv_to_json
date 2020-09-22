# csv_to_json
CSV to JSON transform

##### Table of Contents
- [Why Bother?](#why-bother)
- [CSV](#csv)
  * [Delimiters](#delimiters)
  * [!Disclaimer!](#disclaimer)
  * [Sample](#sample)
- [JSON](#json)
  * [Raw (Unformatted)](#raw-unformatted)
  * [Formatted](#formatted)
- [Transforms](#transforms)
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

# Why Bother?
CSV works great for simple data where the data is relatively flat (table dump, etc.) though for complex data where fields (columns) can repeat, have subfields, etc. CSV breaks down. The transformation of the CSV into something processable is mainly on each comsumer. Enter JSON. JSON works well with simple and complex data. Is supported by most if not all programming languages either natively or via extension. Is relatively light wieght and parsing is generally pretty fast since it powers the web. 

So why not update your process to output JSON and abandon CSV? Thats the hard part. Moving away from a standard encures a cost to consumers that have already figured out your CSV. Maintaining support for mutliple formats can add development | complexity | maintanence costs. Maybe your long term goal is to move away from CSV though doing so isn't feasible within your current tooling | infrastructure. Enter post processing. Post processing your existing CSV to an intermediate can be a reasonable way to leave your existing platform alone while providing data in a richer format that can be transformed | consumed more readily.

So why Python? Python is a simple programming language to learn, has tons of libraries, community support, along with a robust job orchestraton / workflow platform like [Airflow](https://airflow.apache.org/). 

Back to the real motivation. Time. I'm by no means an expert Python programmer. In total I spent around 8 hours coding the transforms. Did I write tests? Not completely... though it wouldn't take too much time to complete. Adding additional transforms is pretty straight forward. Adding the problem transform took around 30 minutes. Most of my time was spent creating the initial (allergy) transform, profiling the code. Being able to write a transform quickly is key if your goal is to be a bridge long term or the transform is specific to a consumer request (one off). 

Final comments. Whether you chose to use Python or not. Implementing JSON is beneficial for cases where consumers (clients) request (pay) for one off mappings to their proprietory spec. Given JSON is easy to work with, writing these one off transforms from JSON will reduce coding time. Using a program language like Python along with a worflow platform like airflow you can create transformation pipelines where the initial step is converting to JSON. JSON also allows for consolidation | grouping of data where the JSON object could echo your CSV where each row has a coresponding JSON object or the object contains similar rows (aka all person allergies vs a single allergy) without having to define a new object (JSON lists). 

So is Python good enough? Maybe... Still not convinced JSON is the way? Should probably stop here unless you're curious.

# CSV 

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
{"ids": [{"id": "12345", "authority": "Hospital MRN", "id_type": "MRN"}, {"id": "67890", "authority": "HIE MRN", "id_type": "CMRN"}], "name": {"last": "ZZLast", "first": "Jane", "middle": "Marie"}, "birth_date": "19500701143000", "admin_sex": {"id": "362", "description": "Female"}, "encounter": {"ids": [{"id": "893727", "authority": "Hospital FIN", "id_type": "FIN"}]}, "allergys": [{"allergen_type": {"id": "Drug"}, "allergen": {"id": "723", "description": "Amoxicillin", "coding_method": "RXCUI"}, "severity": {"id": "SEVERE"}, "onset": "20180724", "reaction_status": {"id": "CANCELED"}, "reaction_class": {"id": "CLASS"}, "source_of_info": {"id": "PARENT"}, "source_of_info_ft": "Parent", "cancel_dt_tm": "20190813165421", "reviewed_dt_tm": "20190813165431", "reactions": [{"code": {"id": "498834018", "description": "Abdominal swelling |~ distended areas", "coding_method": "SNOMED"}, "severity": {"id": "777777", "description": "desc", "coding_method": "SNOMED"}}], "physician": {"id": {"id": "13243", "authority": "NPI"}, "name": {"last": "ZZPhylast", "first": "Robert"}, "phys_type": "REV"}, "comments": [{"text": "Discovered during ER visit ~July|August 2018", "comment_dt_tm": "06/19/1999 12:34:56", "physician": {"id": {"id": "ID1234", "authority": "NPI"}, "name": {"last": "Test1", "first": "Physician1"}}}, {"text": "Bad swelling to \"chest\", head", "comment_dt_tm": "01/01/1991 01:11:11", "physician": {"id": {"id": "ID5678", "authority": "NPI"}, "name": {"last": "Test2", "first": "Physician2"}}}]}]}
```

## Formatted
```
{
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

## Transform.py
The transformation script takes in three arguments. The type of transformation, the "in" directory to scan and the "out" directory to write the json files to. The output file will maintain the original csv file name. Upon completion the csv file will be deleted from the "in" directory.

```
python transform.py type in_dir out_dir
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
I profiled the allergy transform to get an idea of how "bad" or "good" an idea using Python is. Data was collected using cProfile with a 488 MB CSV containing a little over 1M rows. The CSV was constructed from the sample allergy csv.

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
- Additional transforms
- Airflow integration