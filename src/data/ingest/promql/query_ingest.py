import re
import math
import time

from src.program_data.program_data import ProgramData
from src.data.data_repository import DataRepository
from src.data.identifiers.identifier import SourceQueryIdentifier
from data.ingest.ingest_controller import *
from src.data.ingest.grafana_df_analyzer import *
from src.data.processors import process_periods
from src.data.ingest.promql.query_executor import perform_query, transform_query_response
from src.data.ingest.promql.query_designer import build_query_list
from src.utils.timeutils import to_unix_ts, from_unix_ts
from src.data.filters import *

class PromQLIngestController(IngestController):
    def ingest(self) -> DataRepository:
        
        prog_data: ProgramData = self.prog_data
        # The data repository that holds SourceQueryIdentifiers, this is different from the
        #   standard DataRepository because there are multiple queries per period and type, to be
        #   used in the processing step where we only get pending/running pods.
        data_repo: DataRepository = DataRepository()

        query_blocks = build_query_list(prog_data.config, prog_data.args)

        print(f"Loading data from {len(query_blocks)} query/queries:")

        for query_block in query_blocks:
            print(f"  {query_block}")

            # Perform the query and transform the json response into a Grafana DataFrame
            json_response = perform_query(query_block.query_url)
            grafana_df = transform_query_response(json_response)

            # Convert values to numeric
            grafana_df = convert_to_numeric(grafana_df)

            # Read identifying data about DataFrame
            period = get_period(grafana_df)
            resource_type = None
            if(query_block.query_name == "truth"):
                resource_type = get_resource_type(grafana_df)

            identifier = SourceQueryIdentifier(period[0], period[1], resource_type, query_block.query_name)
            
            data_repo.add(identifier, grafana_df)

        # Normalize periods for filtering step, then perform filtering
        data_repo = process_periods(data_repo)

        print("Applying running/pending filter...")

        start_time = time.time()
        data_repo = _filter_to_running_pending(self.prog_data, data_repo)
        end_time = time.time()

        print(f"Filtering took {(end_time-start_time)} seconds.")

        return data_repo

def _filter_to_running_pending(prog_data: ProgramData, data_repo: DataRepository) -> DataRepository:
    """
    Filter a DataRepository containing multiple SourceQueryIdentifiers to SourceIdentifiers
        based off of their running/pending status.

    Args:
        data_repo (DataRepository): The input repository, contains SourceQueryIdentifiers to
            be transformed.
    
    Returns:
        DataRepository: The output DataRepository, contains SourceIdentifiers.
    """

    out_repository = DataRepository()

    step = prog_data.config["step"]

    # Get filter lambdas for both source query types
    source_query_type_lambda = filter_type(SourceQueryIdentifier)
    status_lambda = lambda id: source_query_type_lambda(id) and id.query_name == "status"
    truth_lambda = lambda id: source_query_type_lambda(id) and id.query_name == "truth"

    last_timestamp = time.time()

    for status_identifier in data_repo.filter_ids(status_lambda):
        status_df_raw = data_repo.get_data(status_identifier)
        print("preprocessing status")
        last_timestamp = time.time()
        status_df = _preprocess_df(status_df_raw, False, step)
        print(f"done in {time.time() - last_timestamp}")

        # Tracks the set of created types, used to protect from creating multiple SourceIdentifiers
        #   with the same start_ts, end_ts, and type        
        created_types = set() 

        # Filter identifiers with type SourceQueryIdentifier, query_name=truth, and matching 
        #   timestamps 
        identifiers_filter = lambda identifier: truth_lambda(identifier) and filter_timestamps(status_identifier.start_ts, status_identifier.end_ts)
        identifiers = data_repo.filter_ids(identifiers_filter)

        for values_identifier in identifiers:
            if(values_identifier.type in created_types):
                print(f"ERROR: Identifier of type \"{values_identifier.type}\" is already in created_types for this timestamp range. This shouldn't happen.")
                continue
        
            values_df_raw = data_repo.get_data(values_identifier)
            print(f"preprocessing values for {values_identifier}")
            last_timestamp = time.time()
            values_df = _preprocess_df(values_df_raw, True, step)
            print(f"done in {time.time() - last_timestamp}")
            values_df = _apply_status_df(status_df, values_df)

            identifier = SourceIdentifier(values_identifier.start_ts, values_identifier.end_ts, values_identifier.type)
            out_repository.add(identifier, values_df)         

            created_types.add(identifier.type)   

    return out_repository

def _filter_cols_zero(df: pd.DataFrame):
    """
    Removes columns with a sum of 0, makes processing more efficient later.

    Args:
        df (pd.DataFrame): The DataFrame to remove columns.
    
    Returns:
        pd.DataFrame: The adjusted DataFrame with removed empty columns.
    """
    
    df.drop(columns=df.columns[df.sum() == 0], inplace=True)
    return df    

def _merge_columns_on_uid(df: pd.DataFrame, preserve_columns: bool = False):
    """
    Takes the max value from each uid in the DataFrame and places it in a single row, this
        squashes the DataFrame horizontally for more efficient processing later.
    
    Args:
        df (pd.DataFrame): The DataFrame to merge horizontally.
        preserve_columns (bool): Keep the column names as they were or replace with only UIDs.
    
    Returns:
        pd.DataFrame: The adjusted DataFrame with squashed columns.
    """
    # Holds string uid key with original column name values
    orig_names = {}

    def select_uid(string):
        if(string == "Time"):
            return string

        match = re.search(r'uid="([^"]+)"', string)
        if match:
            uid = match.group(1)

            # Store first instance of the uid's original name, pandas will append .X to duplicate 
            #   column names
            if(preserve_columns and uid not in orig_names):
                orig_names[uid] = string

            return uid
        else:
            raise Exception(f"Failed to read uid in column name \"{string}\"")

    time_col = df['Time'] # Separate Time column
    df = df.drop(columns='Time')

    df.columns = [select_uid(col) for col in df.columns]

    df = df.T.groupby(df.columns).max().T # Merge columns with the same UID

    if(preserve_columns):
        df.columns = [orig_names[uid] for uid in df.columns]

    df.insert(0, 'Time', time_col) # Reattach Time column

    return df

def _infer_times(df: pd.DataFrame, step):
    """
    Infer timestamps in between rows of a Grafana DataFrame based off of a step value.
    If the difference in timestamps between two rows is greater than the step value, new timestamps
        will be generated with the correct step values to fill the gap.

    Args:
        df (pd.DataFrame): The DataFrame that times will need to be inferred for.
        step (int): The time in seconds between expected steps.

    Returns:
        pd.DataFrame: The adjusted DataFrame with inferred time rows.    
    """

    columns_excluding_time = list(df.columns)
    columns_excluding_time.remove("Time")

    i = 0
    while i < len(df["Time"]) - 1:
        
        time = to_unix_ts(df["Time"][i])
        next_time = to_unix_ts(df["Time"][i+1])
        time_offset = next_time-time

        if(time_offset <= step):
            i += 1
            continue

        rows_to_add = math.floor(time_offset/step)

        times_arr = [from_unix_ts(time + j*step) for j in range(1, rows_to_add)]
        times_dict = { "Time": times_arr } 

        rows_arr = [float('NaN')]*len(times_arr)
        rows_dict = { key: rows_arr for key in columns_excluding_time }

        final_df = times_dict | rows_dict

        rows_df = pd.DataFrame(final_df)        
                
        df = pd.concat([df.iloc[:i+1], rows_df, df.iloc[i+1:]]).reset_index(drop=True)

        i += rows_to_add

    return df

def _preprocess_df(df: pd.DataFrame, preserve_columns, step):
    last_timestamp = time.time()
    print("Filtering 0 cols")
    df = _filter_cols_zero(df)
    print(f"done in {(time.time() - last_timestamp):.1f}")
    last_timestamp = time.time()
    print("Merging columns on uid")
    df = _merge_columns_on_uid(df, preserve_columns)
    print(f"done in {(time.time() - last_timestamp):.1f}")
    last_timestamp = time.time()
    print("Inferring times")
    df = _infer_times(df, step)
    print(f"done in {(time.time() - last_timestamp):.1f}")
    return df

def _apply_status_df(status_df, values_df):
    """
    Apply the status DataFrame to the values DataFrame, only accepting values from the values_df
        when the status_df has a 1 in that cell position.
    Cells are identified by column=uid and row=timestamp, so the status_df and the values_df must
        have matching time columns.

    Args:
        status_df (pd.DataFrame): The DataFrame holding running/pending statuses.
        values_df (pd.DataFrame): The DataFrame holding the usage values.

    Returns:
        pd.DataFrame: The values DataFrame with the running/pending statuses applied.
    """

    def select_uid(string):
        if(string == "Time"):
            return string

        match = re.search(r'uid="([^"]+)"', string)
        if match:
            uid = match.group(1)
            return uid
        else:
            raise Exception(f"Failed to read uid in column name \"{string}\"")

    start_ts = to_unix_ts(values_df["Time"][0])
    end_ts = to_unix_ts(list(values_df["Time"])[-1])

    times_list = [to_unix_ts(time) for time in status_df["Time"]]

    try:
        start_index = times_list.index(start_ts)
    except ValueError:
        start_index = 0

    try:
        end_index = times_list.index(end_ts)
    except ValueError:
        end_index = len(times_list)-1

    drop_columns = []

    for column in values_df.columns:
        if(column == "Time"):
            continue

        uid = select_uid(column)

        if(uid not in status_df.columns):
            drop_columns.append(column)
            continue

        status_column = status_df[uid].iloc[range(start_index, end_index+1)]
        status_column.index = values_df.index

        values_df[column] = values_df[column].where(status_column == 1)

    values_df.drop(columns=drop_columns, inplace=True)

    return values_df


