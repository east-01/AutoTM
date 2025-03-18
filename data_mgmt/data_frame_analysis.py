import datetime
import calendar

from program_data.program_data import ProgramData
from utils.timeutils import to_unix_ts, get_unix_timestamp_range

def extract_column_data(col_name):
    """
    Given a column name with the format {label1="value1",label2="value2",...} break it down into a
      usable python dictionary.
    """
    # Strip opening and closing {}
    col_name = col_name[1:-1]
    data = {}

    for pair in col_name.split(', '):
        pairsplit = pair.split('=')
        label = pairsplit[0]
        value = pairsplit[1][1:-1] # Grab the value without the first and last characters to remove quotes.

        data[label] = value

    return data

def analyze_df_type(col_datas):
    """
    Analyze each column data in the DataFrame to get the set of unique resource types.
    The resource type is the string following the resource= tag in the column title.
    It is expected that the DataFrame has uniform resource types among all columns.
    """
    # Create a set of all the type strings, this will give a list of unique type names
    type_set = set()
    for col_data in col_datas:
        if("resource" not in col_data.keys()):
            raise Exception(f"Column \"{col_data}\" doesn't have a resource type.")
        type_set.add(col_data["resource"])

    # If the length of the set is more than one we have an invalid DF
    if(len(type_set) > 1):
        raise Exception(f"Type analysis error: yielded more than one type: {list(type_set)}. It is expected that the DataFrame has consistent resource types between all columns.")

    # Reverse the type string (i.e. "nvidia_com_gpu") to it's associated program type (i.e. "gpu")
    type_string = list(type_set)[0]
    type = None
    
    # Find the program type by finding the dictionary key from its value
    type_strings = ProgramData().settings['type_strings']
    for possible_type in type_strings.keys():
        if(type_strings[possible_type] == type_string):
            type = possible_type
            break

    if(type is None):
        raise Exception(f"Type analysis error: couldn't reverse type string \"{type_string}\" into it's associated program type. [{type_strings.keys()}]")

    return type

def analyze_df_period(df):
    """
    Ensure the DataFrame has a Time column, returning the start and ending times.
    """
    
    if('Time' not in df.columns):
        raise Exception("Period analysis error: \"Time\" not in the columns of the DataFrame.")

    times = list(df['Time'])

    if(len(times) < 1):
        raise Exception("Period analysis error: Time column of length less than 1")
    
    # TODO: Upgrade time period inferring
    # Instead of inferring the time period here, we should look at all time periods and fix their
    #   start and end times to the closest checkpoints. Like this:
    # month_st=0, df_st=12, df_et=30, month_et=45 -> month_st=df_st=0, month_et=df_et=45

    start = to_unix_ts(times[0])
    start_dt = datetime.datetime.fromtimestamp(start)
    start_range = get_unix_timestamp_range(start_dt.month, start_dt.year)
    if(start != start_range[0]):
        print(f"WARN: Inferring time range start to the first second of the month {start} -> {start_range[0]}")
        start = start_range[0]

    end = to_unix_ts(times[-1])
    end_dt = datetime.datetime.fromtimestamp(end)
    end_range = get_unix_timestamp_range(end_dt.month, end_dt.year)
    if(end != end_range[1]):
        print(f"WARN: Inferring time range end to the last second of the month {end} -> {end_range[1]}")
        end = end_range[1]

    if(start_dt.month != end_dt.month or start_dt.year != end_dt.year):
        print("ERROR: Analyzing df's period shows that the start and end times belong to different months. This will most likely yield broken results. Try using a PromQL query instead of input directory.")

    return (start, end)

