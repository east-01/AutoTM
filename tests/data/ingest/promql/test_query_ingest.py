import pytest
import os
import json
import pandas as pd

from src.data.ingest.promql.query_ingest import _filter_cols_zero, _merge_columns_on_uid, _infer_times, _preprocess_df, _apply_status_df, _filter_to_running_pending
from src.data.ingest.grafana_df_analyzer import convert_to_numeric
from src.data.data_repository import DataRepository
from src.data.identifiers.identifier import *
from src.data.filters import *

# Step 0
@pytest.fixture
def status_df(test_files_dir):
    file_name = os.path.join(test_files_dir, "mar25_dualquery", "status.csv")
    df = convert_to_numeric(pd.read_csv(file_name))
    return df

# Test step 1
@pytest.fixture
def status_df_expected_0filtered(test_files_dir):
    file_name = os.path.join(test_files_dir, "mar25_dualquery", "status_0filtered.csv")
    df = convert_to_numeric(pd.read_csv(file_name))
    return df

def test_strip_zero_statuses(status_df, status_df_expected_0filtered):
    filtered: pd.DataFrame = _filter_cols_zero(status_df)

    pd.testing.assert_frame_equal(filtered, status_df_expected_0filtered)
    
# Test step 2
@pytest.fixture
def status_df_expected_merged(test_files_dir):
    file_name = os.path.join(test_files_dir, "mar25_dualquery", "status_merged.csv")
    df = convert_to_numeric(pd.read_csv(file_name))
    return df

def test_merge_columns_on_uid(status_df_expected_0filtered, status_df_expected_merged):
    merged = _merge_columns_on_uid(status_df_expected_0filtered)

    pd.testing.assert_frame_equal(merged, status_df_expected_merged, check_dtype=False)

# Test step 3
@pytest.fixture
def status_df_expected_inferred(test_files_dir):
    file_name = os.path.join(test_files_dir, "mar25_dualquery", "status_inferred.csv")
    df = convert_to_numeric(pd.read_csv(file_name))
    return df

def test_infer_timestamps(status_df_expected_merged, status_df_expected_inferred):
    inferred = _infer_times(status_df_expected_merged, 3600) # 3600 is the step that the mar25_dualquery files were generated with

    # inferred.to_csv("C:/East/work/tidemetrics/tests/test_files/mar25_dualquery/status_inferred_generated.csv", index=False)

    pd.testing.assert_frame_equal(inferred, status_df_expected_inferred, check_dtype=False)

# Application of status
@pytest.fixture
def truth_cpu_in_df(test_files_dir):
    file_name = os.path.join(test_files_dir, "mar25_dualquery", "truth_cpu_in.csv")
    df = convert_to_numeric(pd.read_csv(file_name))
    return df

@pytest.fixture
def truth_cpu_out_df(test_files_dir):
    file_name = os.path.join(test_files_dir, "mar25_dualquery", "truth_cpu_out.csv")
    df = convert_to_numeric(pd.read_csv(file_name))
    return df

@pytest.fixture
def truth_gpu_in_df(test_files_dir):
    file_name = os.path.join(test_files_dir, "mar25_dualquery", "truth_gpu_in.csv")
    df = convert_to_numeric(pd.read_csv(file_name))
    return df

@pytest.fixture
def truth_gpu_out_df(test_files_dir):
    file_name = os.path.join(test_files_dir, "mar25_dualquery", "truth_gpu_out.csv")
    df = convert_to_numeric(pd.read_csv(file_name))
    return df

def test_apply_status_df(status_df, truth_cpu_in_df, truth_cpu_out_df):

    status_df = _preprocess_df(status_df, False, 3600)
    truth_df = _preprocess_df(truth_cpu_in_df, True, 3600)
    
    out_truth_df = _apply_status_df(status_df, truth_df)

    pd.testing.assert_frame_equal(out_truth_df, truth_cpu_out_df)

def test_filter_running_pending(program_data_def_config, status_df, truth_cpu_in_df, truth_cpu_out_df, truth_gpu_in_df, truth_gpu_out_df):
    data_repo: DataRepository = DataRepository()
    data_repo.add(SourceQueryIdentifier(0, 1, None, "status"), status_df)
    data_repo.add(SourceQueryIdentifier(0, 1, "cpu", "truth"), truth_cpu_in_df)
    data_repo.add(SourceQueryIdentifier(0, 1, "gpu", "truth"), truth_gpu_in_df)

    out_data_repo = _filter_to_running_pending(program_data_def_config, data_repo)

    # Ensure output data repo contains only SourceIdentifier cpu and gpu
    assert out_data_repo.count() == 2
    assert len(out_data_repo.filter_ids(filter_type(SourceIdentifier, strict=True))) == 2

    out_cpu = out_data_repo.get_data(SourceIdentifier(0, 1, "cpu")) # MAKE SURE these source identifiers match above
    out_gpu = out_data_repo.get_data(SourceIdentifier(0, 1, "gpu"))

    pd.testing.assert_frame_equal(out_cpu, truth_cpu_out_df)
    pd.testing.assert_frame_equal(out_gpu, truth_gpu_out_df)

@pytest.fixture
def truth_cpu_df(test_files_dir):
    file_name = os.path.join(test_files_dir, "mar25_dualquery", "truth_cpu_in.csv")
    return pd.read_csv(file_name)

@pytest.fixture
def truth_gpu_df(test_files_dir):
    file_name = os.path.join(test_files_dir, "mar25_dualquery", "truth_gpu_in.csv")
    return pd.read_csv(file_name)
