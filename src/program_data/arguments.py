import argparse
import time
import os
import datetime

from src.program_data.settings import settings
from src.utils.timeutils import get_unix_timestamp_range

def load_arguments():
    """
    Using the argparse library, parse the command line arguments into usable data.
    """

    parser = argparse.ArgumentParser(prog='AutoTM', description='Auto Tide Metrics- Collect and visualize tide metrics')

    # These arguments will be populated if defaults arent provided
    # We could use default=__ here, but I want to be able to provide warnings in verify_arguments if necessary.
    parser.add_argument('analysis_options', type=parse_analysis_options, help="A list of analysis options separated by a comma (no spaces).")

    # Ingest options
    ingest_group = parser.add_argument_group("Ingest options", "Options for reading data")
    group = ingest_group.add_mutually_exclusive_group(required=True)
    group.add_argument('-p', '--period', dest='period', type=parse_time_range, help="A time range of the format <start>-<end> where your start and end times are UNIX timestamps.")
    group.add_argument('-f', '--file', dest='file', type=parse_file_list, help="A local file/directory to be used instead of polling Prometheus.")
    ingest_group.add_argument('-u', '--users', dest='users', action='store_true', help="Ingest users from JupyterHub sources specified in config.")

    # Output options
    output_group = parser.add_argument_group("Output options", "Options for data output")
    output_group.add_argument('-o', '--outdir', dest='outdir', type=str, help="The directory to send output files to.")

    parser.add_argument('-v', dest='verbose', action='store_true', help="Enable verbose output.")

    return parser.parse_args()

def verify_arguments(prog_data):
    args = prog_data.args

    if(not isinstance(args.analysis_options, list)):
        raise ArgumentException("Analysis options is not a list.")

    # Populate additional analyses to perform from requirements
    for to_perform in args.analysis_options:
        for requirement in prog_data.settings["analysis_settings"][to_perform]["requires"]:
            if(requirement not in args.analysis_options):
                print(f"Added additional analysis \"{requirement}\" as it is a requirement of \"{to_perform}\"")
                args.analysis_options.append(requirement)

    if(args.file is not None and args.period is not None):
        # If the file exists, provide warnings about other arguments that won't be used
        raise ArgumentException("Both file and period arguments provided, these arguments are mutually exclusive and you must select one.")

    if(args.period is not None):
        now = int(time.time())
        if(args.period[0] > now or args.period[1] > now):
            raise ArgumentException("Either the start or end times of the period exceeds current time.")

class ArgumentException(Exception):
    pass

def is_integer(value):
    """
    Ensure a python value is either a true integer or an integer string.
    """
    if isinstance(value, int):
        return True
    if isinstance(value, str):
        return value.isdigit() or (value.startswith('-') and value[1:].isdigit())
    return False

def parse_unix_ts_range(time_range_str):
    """
    Parse a time range with the format <start>-<end> with both the start and end times being
    UNIX Timestamps.
    """
    # Ensure delimiter exists
    if('-' not in time_range_str):
        print(f"Failed to parse time range string \"{time_range_str}\" it doesn't have the \"-\" required for splitting into start and end time.")
        raise ValueError()
    
    # Split the argument and ensure beginning and end are integers
    time_range_arr = time_range_str.split('-')
    if(not is_integer(time_range_arr[0]) or not is_integer(time_range_arr[1])):
        print(f"Failed to parse time range string \"{time_range_str}\" either the start or end time failed to parse as Integer.")
        raise ValueError()
    
    return (int(time_range_arr[0]), int(time_range_arr[1]))

def parse_month_year(time_str):
    try:
        month_year = datetime.datetime.strptime(time_str, '%B%y')
        return get_unix_timestamp_range(month_year.month, month_year.year)
    except ValueError as e:
        return None

def parse_time_range(time_range_str):
    # Parse the time range string as a single year (i.e. 2024, 2025)
    try:
        year_num = int(time_range_str)
        # Ensure the year is in modern era
        if(year_num >= 2000 and year_num < 4000):
            jan = get_unix_timestamp_range(1, year_num)
            dec = get_unix_timestamp_range(12, year_num)
            return (jan[0], dec[1])
    except:
        pass

    # Try to parse the time range string as either a single month or a range of months (i.e. January24 or January24-March24)
    if("-" in time_range_str):
        time_str_arr = time_range_str.split("-")
        if(len(time_str_arr) == 2):
            start = parse_month_year(time_str_arr[0])
            end = parse_month_year(time_str_arr[1])
            return (start[0], end[1])
    else:
        single_month_year = parse_month_year(time_range_str)
        if(single_month_year):
            return single_month_year

    return parse_unix_ts_range(time_range_str)

def parse_analysis_options(analysis_options_str):
    if(analysis_options_str == "all"):
        return list(settings["analysis_settings"].keys())

    options = analysis_options_str.split(",")
    for option in options:
        if(option not in settings["analysis_settings"].keys()):
            print(f"Failed to parse analysis option list, \"{option}\" is not recognized as a valid option.")
            raise ValueError()
    return options

def parse_file_list(path):
    if(os.path.isfile(path)):
        return [path]
    elif(os.path.isdir(path)):
        file_paths = []
        for root, _, files in os.walk(path):
            for file in files:
                file_paths.append(os.path.join(root, file))
        return file_paths
    else:
        raise ArgumentException(f"Can't parse file list, path provided \"{path}\" is neither a file or directory.")