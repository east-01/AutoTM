import os
import pytest
import sys
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.data.identifiers.identifier import *
from src.data.data_repository import DataRepository
from src.data.filters import filter_type, filter_source_type, filter_analyses_of

class TestDataRepository:
    def setup_method(self, method):
        self.srcid1 = SourceIdentifier(0, 10, "cpu")
        self.srcid2 = SourceIdentifier(11, 20, "cpu")
        self.srcid3 = SourceIdentifier(21, 30, "gpu")
        self.aid1 = AnalysisIdentifier(self.srcid3, "gpuhours")
        self.aid2 = AnalysisIdentifier(self.srcid2, "cpuhours")
        self.vis1 = VisIdentifier(self.aid1, "horizontalbar")

        self.repo = DataRepository()

        self.repo.add(self.srcid1, "test1")
        self.repo.add(self.srcid2, "test2")
        self.repo.add(self.srcid3, "test3")
        self.repo.add(self.aid1, "aid1")
        self.repo.add(self.aid2, "aid2")
        self.repo.add(self.vis1, "vis1")

    def teardown_method(self, method):
        pass

    def test_add_non_identifier(self):
        with pytest.raises(ValueError):
            self.repo.add("notsrcid", "value")

    def test_contains(self):
        assert self.repo.contains(self.srcid1)

    def test_not_contains(self):
        assert not self.repo.contains(SourceIdentifier(0, 1, "cpu"))

    def test_count(self):
        assert self.repo.count() == 6

    def test_filter_type(self):
        assert len(self.repo.filter_ids(filter_type(SourceIdentifier))) == 3

    def test_filter_source_type(self):
        assert len(self.repo.filter_ids(filter_source_type("cpu"))) == 2

    def test_filter_source_type_gpu(self):
        assert len(self.repo.filter_ids(filter_source_type("gpu"))) == 1

    def test_filter_analysis_of(self):
        assert len(self.repo.filter_ids(filter_analyses_of(self.srcid3))) == 1

class TestPromQLDataRepository:
    """
    The point of these tests is to see if we can tell the difference between SourceQueryIdentifiers
      and SourceIdentifiers
    """
    def setup_method(self, method):
        self.srcid1 = SourceIdentifier(0, 10, "cpu")
        self.srcid2 = SourceQueryIdentifier(11, 20, "cpu", "test")
        self.srcid3 = SourceQueryIdentifier(21, 30, "gpu", "test")

        self.repo = DataRepository()

        self.repo.add(self.srcid1, "test1")
        self.repo.add(self.srcid2, "test2")
        self.repo.add(self.srcid3, "test3")

    def teardown_method(self, method):
        pass

    def test_filter_type_not_strict(self):
        assert len(self.repo.filter_ids(filter_type(SourceIdentifier))) == 3

    def test_filter_type_strict(self):
        assert len(self.repo.filter_ids(filter_type(SourceIdentifier, True))) == 1