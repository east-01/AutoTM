# The data loader observer will take in the command line arguments and retrieve the corresponding
#   data. The observer currently checks arguments:
# type (cpu/gpu/uniquens): For the type of data to retrieve
# file [optional]: The file to pull the data from

from src.program_data.program_data import ProgramData
from src.data.data_repository import DataRepository

from src.data.ingest.promql.query_ingest import PromQLIngestController
from src.data.ingest.filesystem.fs_ingest import FileSystemIngestController

def ingest(prog_data: ProgramData):
    """
    Observes the state of args and builds the corresponding DataRepository object.
    """

    # Create standard DataRepository that will be used for analyses, added to by IngestControllers
    data_repo: DataRepository = DataRepository()    
    
    # Loop through each ingest controller, perform its ingest action, then join the resulting data
    #   to the standard DataRepository.
    for ingest_controller in _load_ingest_controllers(prog_data):
        ingested_data_repo = ingest_controller.ingest()

        try:
            data_repo.join(ingested_data_repo)
        except ValueError as e:
            print(f"Error when joining data from ingest controller of type \"{type(ingest_controller)}\": {e}")

    # Ensure the DataRepository loaded properly
    if(data_repo.count() == 0):
        raise Exception(f"Failed to load DataRepository. The repo is empty.")

    print(f"Loaded {data_repo.count()} data frame(s).")

    return data_repo

def _load_ingest_controllers(prog_data: ProgramData):
    ics = []

    if(prog_data.args.file is not None):
        ics.append(FileSystemIngestController(prog_data))
    else:
        ics.append(PromQLIngestController(prog_data))

    # TODO: Add once user ingest is in place
    # if(prog_data.args.users):
    #     ics.append()
    
    return ics