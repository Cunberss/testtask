import json
from jsonschema import validate, ValidationError


def validate_input(input_string: str):
    schema = {
        "type" : "object",
        "properties" : {
            "dt_from" : {"type" : "string", "format": "date-time"},
            "dt_upto" : {"type" : "string", "format": "date-time"},
            "group_type" : {"type" : "string", "enum": ["hour", "day", "month"]}
        },
        "required": ["dt_from", "dt_upto", "group_type"]
    }
    try:
        input_dict = json.loads(input_string)
    except json.JSONDecodeError as e:
        return False

    try:
        validate(input_dict, schema)
    except ValidationError as e:
        return False

    return input_dict
