import pandas as pd

from src.program_data.program_data import ProgramData
from src.data.data_repository import DataRepository
from data.ingest.ingest_controller import *
from src.data.ingest.grafana_df_analyzer import *
from src.data.identifiers.identifier import SourceIdentifier

class FileSystemIngestController(IngestController):
    def ingest(self) -> DataRepository:
        
        prog_data: ProgramData = self.prog_data
        data_repo: DataRepository = DataRepository()

        input_directory = prog_data.args.file
        print(f"Loading data from {len(input_directory)} file(s):")
        
        for file_path in input_directory:
            print(f"  {file_path}")

            file_df = pd.read_csv(file_path)
            # Convert values to numeric
            file_df = convert_to_numeric(file_df)

            # Read identifying data about DataFrame
            period = get_period(file_df)
            resource_type = get_resource_type(file_df)

            identifier = SourceIdentifier(period[0], period[1], resource_type)

            data_repo.add(identifier, file_df)