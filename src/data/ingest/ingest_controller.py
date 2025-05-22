from abc import ABC, abstractmethod

from src.program_data.program_data import ProgramData
from src.data.data_repository import DataRepository

class IngestController(ABC):
    def __init__(self, prog_data: ProgramData):
        self.prog_data = prog_data
    
    @abstractmethod
    def ingest(self) -> DataRepository:
        """
        Ingest data from a source.

        Returns:
            DataRepository: The ingested information.
        """
        pass