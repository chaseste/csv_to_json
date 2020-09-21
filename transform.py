""" Transformation script """

import sys
import os
import csv_to_json.transformers as transformers

def main():
    if len(sys.argv) != 4:
        print("Usage: python transform.py type in_dir out_dir")
        sys.exit(1)

    dispatch = { 
        "ALLERGY": transformers.AllergyToJson(),
        "PROBLEM": transformers.ProblemToJson()
    }
    if not sys.argv[1] in dispatch:
        print("Invalid transformation type.\n\nSupported Types:\nALLERGY, PROBLEM")
        sys.exit(1)

    src_dir, dest_dir = sys.argv[2], sys.argv[3]
    join, remove = os.path.join, os.remove
    for filename in list(filter(lambda f: f.endswith(".csv"), os.listdir(src_dir))):
        src = join(src_dir, filename)
        with open(src) as f_csv, open(join(dest_dir, "".join([filename[:-3], "json"])), "w") as f_json:
            dispatch[sys.argv[1]].csv_to_json(f_csv, f_json)
        remove(src)

if __name__ == "__main__":
    main()
