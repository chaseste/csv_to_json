""" CSV to JSON Transformation """
from typing import List, Dict
from .csv_transfomer import (
    CsvToJson,
    transform_string,
    transform_string_optional,
    transform_code,
    transform_code_optional,
    transform_codes_optional,
    transform_ids,
    transform_name,
    transform_physician,
    transform_reactions,
    transform_comments,
    is_same_person
)

class AllergyToJson(CsvToJson):
    """ Allergy CSV to Json Transformer """

    __fields__ = 19

    def combine(self, trans: Dict, next_trans: Dict) -> bool:
        if is_same_person(trans, next_trans):
            allergies = trans["allergys"]
            allergies.append(next_trans["allergys"][0])
            return True
        return False

    def transform(self, fields: List[List[str]]) -> Dict:
        trans = {
            "patient": {
                "ids": transform_ids(fields[1]),
                "name": transform_name(fields[2]),
                "birth_date": transform_string(fields[3]),
                "admin_sex": transform_code(fields[4])
            },
            "encounter": {
                "ids": transform_ids(fields[5])
            },
            "allergys": [{
                "allergen_type": transform_code(fields[6]),
                "allergen": transform_code(fields[7]),
            }]
        }
        allergy = trans["allergys"][0]
        transform_code_optional(fields[8], allergy, "severity")
        transform_string_optional(fields[9], allergy, "onset")
        transform_code_optional(fields[10], allergy, "reaction_status")
        transform_code_optional(fields[11], allergy, "reaction_class")
        transform_code_optional(fields[12], allergy, "source_of_info")
        transform_string_optional(fields[13], allergy, "source_of_info_ft")
        transform_string_optional(fields[14], allergy, "cancel_dt_tm")
        transform_string_optional(fields[15], allergy, "reviewed_dt_tm")
        transform_reactions(fields[16], allergy)
        transform_physician(fields[17], allergy)
        transform_comments(fields[18], allergy)
        return trans

class ProblemToJson(CsvToJson):
    """ Problem CSV to Json Transformer """

    __fields__ = 26

    def combine(self, trans: Dict, next_trans: Dict) -> bool:
        if is_same_person(trans, next_trans):
            problems = trans["problems"]
            problems.append(next_trans["problems"][0])
            return True
        return False

    def transform(self, fields: List[List[str]]) -> Dict:
        trans = {
            "patient": {
                "ids": transform_ids(fields[1]),
                "name": transform_name(fields[2]),
                "birth_date": transform_string(fields[3]),
                "admin_sex": transform_code(fields[4])
            },
            "problems": [{
                "action_dt_tm": transform_string(fields[5]),
                "condition": transform_code(fields[6]),
            }]
        }
        problem = trans["problems"][0]
        transform_codes_optional(fields[7], problem, "management_discipline")
        transform_code_optional(fields[8], problem, "persistence")
        transform_code_optional(fields[9], problem, "confirmation_status")
        transform_code_optional(fields[10], problem, "life_cycle_status")
        transform_string_optional(fields[11], problem, "status_dt_tm")
        transform_string_optional(fields[12], problem, "onset_dt_tm")
        transform_code_optional(fields[13], problem, "ranking")
        transform_code_optional(fields[14], problem, "certainty")
        transform_code_optional(fields[15], problem, "individual_awareness")
        transform_code_optional(fields[16], problem, "prognosis")
        transform_code_optional(fields[17], problem, "individual_awareness_prognosis")
        transform_code_optional(fields[18], problem, "family_awareness")
        transform_code_optional(fields[19], problem, "classification")
        transform_code_optional(fields[20], problem, "cancel_reason")
        transform_code_optional(fields[21], problem, "severity")
        transform_code_optional(fields[22], problem, "severity_class")
        transform_comments(fields[23], problem)
        transform_physician(fields[24], problem)
        transform_string_optional(fields[25], problem, "annotated_display")
        return trans
