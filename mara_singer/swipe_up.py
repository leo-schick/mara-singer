"""Helper functions for Swipe up of old records when e.g. using a view attribution window"""

from datetime import datetime, timezone, timedelta

from .commands.files import FileFormat

def calc_last_datetime_from_attribution_window(attribution_window_days: int):
    """Calculates the last datetime from a attribution window based from the current utc date"""
    return datetime.now(timezone.utc).date() - timedelta(days=attribution_window_days)

def get_first_datetime_from_file(file_path: str, file_format: FileFormat, field: str, datetime_format: str) -> datetime:
    """
    Gets the first date time from a file. This is handy when you want to sync. a file but you don't
    know what attribution window has been used to download the data.

    Args:
        file_path: the complete path to the file
        file_format: the file format
        field: the date field in the format. Note: When using a complex data type (e.g. JSON),
                    the field must be on the first level, not on a sub-level
        datetime_format: the date format which shall be used when parsing
    """
    with open(file_path,'r') as f:
        first_row = f.readline()

    if file_format == FileFormat.JSONL:
        if not first_row:
            return None
        import json
        first_record = json.loads(first_row)
        if field in first_record:
            field_value = first_record[field]
            return datetime.strptime(field_value, datetime_format)
    else:
        raise Exception(f'Unknown or not implemented file format: {file_format}')

def compose_delete_from_date(db_alias: str, target_table: str, date_column: str, last_datetime: datetime) -> str:
    """Composes a sql DELETE query to drop old records up to a given date"""
    last_datetime_str = last_datetime.strftime('%Y-%m-%d')
    return f"DELETE FROM {target_table} WHERE {date_column} >= '{last_datetime_str}'"
