from .csv_transfomer import (
    CsvToJson,
    transform_string,
    transform_string_optional,
    transform_code,
    transform_code_optional,
    transform_codes,
    transform_codes_optional,
    transform_ids,
    transform_name,
    transform_physician,
    transform_reactions,
    transform_comments
)

class AllergyToJson(CsvToJson):
    """ Allergy CSV to Json Transformer """

    __fields__ = 19

    def transform(self, fields):
        trans = {
            "ids": transform_ids(fields[1]),
            "name": transform_name(fields[2]),
            "birth_date": transform_string(fields[3]),
            "admin_sex": transform_code(fields[4]),
            "encounter": {
                "ids": transform_ids(fields[5])
            },
            "allergys": {
                "allergen_type": transform_code(fields[6]),
                "allergen": transform_code(fields[7]),
            }
        }
        allergys = trans["allergys"]
        transform_code_optional(fields[8], allergys, "severity")
        transform_string_optional(fields[9], allergys, "onset")
        transform_code_optional(fields[10], allergys, "reaction_status")
        transform_code_optional(fields[11], allergys, "reaction_class")
        transform_code_optional(fields[12], allergys, "source_of_info")
        transform_string_optional(fields[13], allergys, "source_of_info_ft")
        transform_string_optional(fields[14], allergys, "cancel_dt_tm")
        transform_string_optional(fields[15], allergys, "reviewed_dt_tm")
        transform_reactions(fields[16], allergys)
        transform_physician(fields[17], allergys)
        transform_comments(fields[18], allergys)
        return trans

class ProblemToJson(CsvToJson):
    """ Problem CSV to Json Transformer """

    __fields__ = 26

    def transform(self, fields):
        trans = {
            "ids": transform_ids(fields[1]),
            "name": transform_name(fields[2]),
            "birth_date": transform_string(fields[3]),
            "admin_sex": transform_code(fields[4]),
            "problems": {
                "action_dt_tm": transform_string(fields[5]),
                "condition": transform_code(fields[6]),
            }
        }
        problems = trans["problems"]
        transform_codes_optional(fields[7], problems, "management_discipline")
        transform_code_optional(fields[8], problems, "persistence")
        transform_code_optional(fields[9], problems, "confirmation_status")
        transform_code_optional(fields[10], problems, "life_cycle_status")
        transform_string_optional(fields[11], problems, "status_dt_tm")
        transform_string_optional(fields[12], problems, "onset_dt_tm")
        transform_code_optional(fields[13], problems, "ranking")
        transform_code_optional(fields[14], problems, "certainty")
        transform_code_optional(fields[15], problems, "individual_awareness")
        transform_code_optional(fields[16], problems, "prognosis")
        transform_code_optional(fields[17], problems, "individual_awareness_prognosis")
        transform_code_optional(fields[18], problems, "family_awareness")
        transform_code_optional(fields[19], problems, "classification")
        transform_code_optional(fields[20], problems, "cancel_reason")
        transform_code_optional(fields[21], problems, "severity")
        transform_code_optional(fields[22], problems, "severity_class")
        transform_comments(fields[23], problems)
        transform_physician(fields[24], problems)
        transform_string_optional(fields[25], problems, "annotated_display")
        return trans
