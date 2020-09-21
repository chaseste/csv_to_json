from setuptools import setup, Extension
from Cython.Build import cythonize

ext_modules = cythonize([
    Extension("csv_to_json.csv_reader", ["csv_to_json/csv_reader.py"]),
    Extension("csv_to_json.csv_transfomer", ["csv_to_json/csv_transfomer.py"]),
    Extension("csv_to_json.transfomers", ["csv_to_json/transformers.py"])],)

setup(
    name='csv_to_json',
    version='0.0.0',
    description='CSV to JSON transforms',
    ext_modules=ext_modules
)