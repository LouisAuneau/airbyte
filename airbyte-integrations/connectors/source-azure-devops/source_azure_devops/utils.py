import re

def round_datetimes_ms(record: dict, json_schema: dict) -> dict:
    """
    Truncate datetime fields with 7 digits of milliseconds to 6 digits (or less) for all datetime fields recursively.

    Args:
        record: Record to be processed.
        json_schema: JSON schema of the record.
    
    Returns:
        Processed record.
    """
    for field in json_schema.get("properties", {}).keys():
        if field in record and json_schema["properties"][field].get("format") == "date-time" and isinstance(record[field], str):
            record[field] = _round_datetime_ms(record[field])
        elif field in record and json_schema["properties"][field].get("type") == "object":
            record[field] = round_datetimes_ms(record[field], json_schema["properties"][field])
    return record

def _round_datetime_ms(datetime: str) -> str:
    """
    Azure DevOps API returns datetime with 7 digits of milliseconds, which isn't supported by most systems (6 maximum).
    This function truncates the datetime to 6 digits of milliseconds.

    Args:
        datetime: Datetime string to be rounded.
    
    Returns:
        Datetime string with 6 digits (or less) of milliseconds.
    """
    matching = re.match(r"(?P<datetime>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.)(?P<milliseconds>\d{6,7})Z", datetime)
    if matching and len(matching.group("milliseconds")) == 7:
        return matching.group("datetime") + matching.group("milliseconds")[:6] + "Z"
    return datetime