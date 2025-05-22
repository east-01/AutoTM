# import pytest

# from data.ingest.ingest import ingest, _load_dfs, read_grafana_df
# from src.data.identifiers.identifier import *

# @pytest.skip
# def test_load_dfs_single_file(file_args_single_file, cpu_df):
#     dfs = _load_dfs(file_args_single_file)
#     assert len(dfs) == 1
#     assert dfs[0].equals(cpu_df)

# @pytest.skip
# def test_load_dfs_single_month(file_args_single_month, cpu_df, gpu_df):
#     dfs = _load_dfs(file_args_single_month)
#     assert len(dfs) == 2
#     assert dfs[0].equals(cpu_df)
#     assert dfs[1].equals(gpu_df)

# @pytest.skip
# def test_ingest_grafana_df_cpu(cpu_df):
#     df, obj1 = read_grafana_df(cpu_df)
#     obj2 = SourceIdentifier(1704096000, 1706284800, "cpu")
#     assert obj1 == obj2

# @pytest.skip
# def test_ingest_grafana_df_gpu(gpu_df):
#     df, identifier = read_grafana_df(gpu_df)
#     assert identifier == SourceIdentifier(1704736800, 1706284800, "gpu")

# @pytest.skip
# def test_ingest_malformed_file_args(file_args_malformed):
#     with pytest.raises(FileNotFoundError):
#         ingest(file_args_malformed)

# @pytest.skip
# def test_ingest_single_file(file_args_single_file):
#     data_repo = ingest(file_args_single_file)
#     assert data_repo.count() == 1

# @pytest.skip
# def test_ingest_single_month(file_args_single_month):
#     data_repo = ingest(file_args_single_month)
#     assert data_repo.count() == 2

# @pytest.skip
# def test_ingest_multi_month(file_args_multi_month):
#     data_repo = ingest(file_args_multi_month)
#     assert data_repo.count() == 6