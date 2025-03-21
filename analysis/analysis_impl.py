# Each analysis implementation should take in a data_block dictionary and return either a 
#   dataframe, list, or float
# Most of this code is repackaged from Tide2.ipynb in https://github.com/SDSU-Research-CI/rci-helpful-scripts

import pandas as pd

from program_data.program_data import ProgramData
from analysis.data_cleaning import clear_duplicate_uids, clear_blacklisted_uids, has_time_column, clear_time_column
from data_mgmt.data_frame_analysis import extract_column_data

#region Hours
def analyze_hours_byns(data_block):
    df = data_block['data']
    if(has_time_column(df)):
        df = clear_time_column(df)

    # Extract namespaces from column names, excluding the first since that's the Time column
    namespaces = df.columns.str.extract(r'namespace="([^"]+)"')[0]

    # Calculate the sum for each namespace
    namespace_totals = {}
    for namespace in namespaces.unique():
        namespace_df = df.filter(regex=f'namespace="{namespace}"', axis=1)

        namespace_total = namespace_df.sum(axis=1).sum()
        namespace_totals[namespace] = namespace_total

    namespace_totals_df = pd.DataFrame(list(namespace_totals.items()), columns=["Namespace", "Hours"])

    # Drop NA and 0 values
    namespace_totals_df.dropna(inplace=True)
    namespace_totals_df = namespace_totals_df[namespace_totals_df["Hours"] >= 0.001]

    # Sort the final DataFrame by hours
    namespace_totals_df.sort_values(by="Hours", ascending=False, inplace=True)

    return namespace_totals_df

def analyze_hours_total(data_block):
    # Retrieve the corresponding analysis thats already been performed
    analysis_repo = ProgramData().analysis_repo
    identifier = (data_block['period'], data_block['type'])
    hours_df = analysis_repo[identifier][f"{data_block['type']}hours"]

    return hours_df['Hours'].sum()
#endregion

def print_uids(df):
    if(has_time_column(df)):
        df = clear_time_column(df)

    uids = []
    for column in df.columns:
        col_data = extract_column_data(column)
        uids.append(col_data['uid'])
    print(f"UIDS {len(uids)}:", ", ".join(sorted(uids)))

#region Jobs
def _analyze_jobs_final(df, strip_cols_0=True):
    df = df.fillna(0)

    # Create a data frame where the columns 
    if(strip_cols_0):
        greater_than_zero_columns = [col for col in df.columns if df[col].sum() > 0]

        df = df[greater_than_zero_columns].fillna(0)

    #extract all the namespaces and use them as columns, group and summing the values 
    columns = df.columns.str.extract(r'namespace="([^"]+)"')[0]

    namespace_counts_sorted = pd.DataFrame(columns.value_counts().sort_values(ascending=False)).reset_index()
    namespace_counts_sorted.columns = ["Namespace", "Count"]

    return namespace_counts_sorted

def analyze_jobs(data_block):
    df = data_block['data']
    if(has_time_column(df)):
        df = clear_time_column(df)

    # Load data frame, dropping columns with duplicate uids
    df = clear_duplicate_uids(data_block['data'])
    return _analyze_jobs_final(df)

def analyze_cpu_only_jobs(data_block):
    # We have to locate the corresponding GPU data block
    # The UIDs in the GPU will be used to clear blacklisted IDs
    gpu_identifier = (data_block['period'], "gpu")
    gpu_data_block = ProgramData().data_repo.data_blocks[gpu_identifier]

    if(gpu_data_block is None):
        raise Exception("Failed to analyze cpu only jobs, the corresponding gpu data_block could not be found.")

    df = data_block['data']
    if(has_time_column(df)):
        df = clear_time_column(df)

    gpu_df = gpu_data_block['data']
    if(has_time_column(gpu_df)):
        gpu_df = clear_time_column(gpu_df)

    gpu_greater_than_zero_cols = [col for col in gpu_df.columns if gpu_df[col].sum() > 0]
    gpu_df = gpu_df[gpu_greater_than_zero_cols].fillna(0)
    gpu_uuid = set(gpu_df.columns.str.extract(r'uid="([^"]+)"')[0].dropna())

    df = clear_duplicate_uids(df)
    df = clear_blacklisted_uids(df, gpu_uuid)

    return _analyze_jobs_final(df, True)

def analyze_jobs_total(data_block):
    # Retrieve the corresponding analysis thats already been performed
    analysis_repo = ProgramData().analysis_repo
    identifier = (data_block['period'], data_block['type'])
    jobs_df = analysis_repo[identifier][f"{data_block['type']}jobs"]

    return jobs_df['Count'].sum()

def analyze_all_jobs_total(data_block):
    # Retrieve the corresponding analysis thats already been performed
    analysis_repo = ProgramData().analysis_repo
    cpu_jobs_tot = analysis_repo[(data_block['period'], 'cpu')][f"cpujobstotal"]
    gpu_jobs_tot = analysis_repo[(data_block['period'], 'gpu')][f"gpujobstotal"]

    return cpu_jobs_tot + gpu_jobs_tot
#endregion

#region Unique NS
def analyze_unique_ns(data_block):
    # Unique namespace analysis is performed on the cpu type (see analysis_options['types'], cpu is first)
    gpu_identifier = (data_block['period'], "gpu")
    gpu_data_block = ProgramData().data_repo.data_blocks[gpu_identifier]

    if(gpu_data_block is None):
        raise Exception("Failed to analyze unique namespaces, the corresponding gpu data_block could not be found.")

    cpu_df = data_block['data']
    if(has_time_column(cpu_df)):
        cpu_df = clear_time_column(cpu_df)

    gpu_df = gpu_data_block['data']
    if(has_time_column(gpu_df)):
        gpu_df = clear_time_column(gpu_df)

    cpu_namespaces = set(cpu_df.columns.str.extract(r'namespace="([^"]+)"')[0])
    gpu_namespaces = set(gpu_df.columns.str.extract(r'namespace="([^"]+)"')[0])

    unique_ns = list(cpu_namespaces | gpu_namespaces)

    return sorted(unique_ns)

def analyze_unique_ns_count(data_block):
    # Retrieve the corresponding analysis thats already been performed
    analysis_repo = ProgramData().analysis_repo
    identifier = (data_block['period'], data_block['type'])
    ns_list = analysis_repo[identifier]["uniquenslist"]

    return len(ns_list)
#endregion