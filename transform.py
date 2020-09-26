""" Transformation script """
import sys
import os
import csv_to_json.transformers as transformers

def main():
    if len(sys.argv) < 4:
        print("Usage: python transform.py type in_dir out_dir option")
        sys.exit(1)

    combine = "-C" == sys.argv[4].upper() if len(sys.argv) == 5 else False
    dispatch = { 
        "ALLERGY": transformers.AllergyToJson(combine=combine),
        "PROBLEM": transformers.ProblemToJson(combine=combine)
    }

    trans_type = sys.argv[1].upper()
    if not trans_type in dispatch:
        print("Invalid transformation type.\n\nSupported Types:\nALLERGY, PROBLEM")
        sys.exit(1)

    src_dir, dest_dir = sys.argv[2], sys.argv[3]
    join, remove, transformer = os.path.join, os.remove, dispatch[trans_type]
    for filename in list(filter(lambda f: f.endswith(".csv"), os.listdir(src_dir))):
        src, dest = join(src_dir, filename), join(dest_dir, "".join([filename[:-3], "json"]))
        with open(src) as f_csv, open(dest, "w") as f_json:
            transformer.csv_to_json(f_csv, f_json)
        remove(src)

if __name__ == "__main__":
    main()
