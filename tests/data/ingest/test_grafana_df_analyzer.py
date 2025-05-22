import pytest
import pandas as pd

from src.data.ingest.grafana_df_analyzer import get_period, get_resource_type, _extract_column_data

def test_extract_column_data_meaningless():
    data = _extract_column_data("{label1=\"value1\", label2=\"value2\"}")

    assert data["label1"] == "value1"
    assert data["label2"] == "value2"
    assert len(data.keys()) == 2

@pytest.fixture
def column_data():
    return "{container=\"chp\", instance=\"10.244.17.213:8443\", job=\"kube-state-metrics\", namespace=\"sdsu-rci-jh\", node=\"rci-nrp-gpu-05.sdsu.edu\", phase=\"Running\", pod=\"proxy-6c5688584b-kxptk\", prometheus=\"monitoring/k8s\", resource=\"cpu\", uid=\"65f8e2f3-c9be-4461-87a8-e8329a788499\", unit=\"core\"}"

def test_extract_column_data(column_data):
    data = _extract_column_data(column_data)

    assert data["container"] == "chp"
    assert data["instance"] == "10.244.17.213:8443"
    assert data["job"] == "kube-state-metrics"
    assert data["namespace"] == "sdsu-rci-jh"
    assert data["node"] == "rci-nrp-gpu-05.sdsu.edu"
    assert data["phase"] == "Running"
    assert data["pod"] == "proxy-6c5688584b-kxptk"
    assert data["prometheus"] == "monitoring/k8s"
    assert data["resource"] == "cpu"
    assert data["uid"] == "65f8e2f3-c9be-4461-87a8-e8329a788499"
    assert data["unit"] == "core"
    assert len(data.keys()) == 11

def test_get_resource_type_cpu(cpu_df):
    assert get_resource_type(cpu_df) == "cpu"

def test_get_resource_type_gpu(gpu_df):
    assert get_resource_type(gpu_df) == "gpu"

def test_get_resource_type_malformed(malformed_df):
    with pytest.raises(Exception):
        get_resource_type(malformed_df)

def test_get_period_cpu(cpu_df):
    start, end = get_period(cpu_df)
    
    assert start == 1704096000 # 1/1/2024 0:00
    assert end == 1706284800 # 1/26/2024 8:00

def test_get_period_gpu(gpu_df):
    start, end = get_period(gpu_df)

    assert start == 1704736800 # 1/8/2024 10:00
    assert end == 1706284800 # 1/26/2024 8:00

def test_get_period_empty():
    with pytest.raises(Exception):
        get_period(pd.DataFrame)