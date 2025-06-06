import os
import pandas as pd

from src.program_data.program_data import ProgramData
from src.data.data_repository import DataRepository
from src.data.filters import *
from src.data.saving.saver import Saver
from src.utils.fileutils import append_line_to_file

class AnalysisSaver(Saver):
    def __init__(self, prog_data: ProgramData):
        self.prog_data = prog_data

    def save(self):

        data_repo: DataRepository = self.prog_data.data_repo

        outdir = self.prog_data.args.outdir
        self.out_path = os.path.join(outdir, "analysis", "")
        if(not os.path.exists(self.out_path)):
            os.mkdir(self.out_path)

        # Keep a list of files we've written to so we can clear the contents previously existing files
        self.has_written = set()
        # Keep a dict of SourceIdentifiers and their corresponding text results
        self.text_results = {}

        for identifier in data_repo.filter_ids(filter_type(AnalysisIdentifier)):
            identifier: AnalysisIdentifier = identifier

            result = data_repo.get_data(identifier)

            # Only save results that exist
            if(result is None):
                return

            if(not identifier.is_meta_analysis()):
                self.save_analysis(identifier)
            else:
                self.save_meta_analysis(identifier)

        # Save text results, order keys by start time
        ordered_keys = sorted(self.text_results.keys(), key=lambda x: x.start_ts)
        for identifier in ordered_keys:
            metadata = data_repo.get_metadata(identifier)

            if(len(self.text_results[identifier]) > 0):
                path = os.path.join(self.out_path, f"text_results.txt")
                to_append = f"For {identifier.type}-{metadata["readable_period"]}:\n  {"\n  ".join(self.text_results[identifier])}"

                append_line_to_file(path, to_append, path not in self.has_written)
                self.has_written.add(path)

    def save_analysis(self, identifier: AnalysisIdentifier):

        data_repo: DataRepository = self.prog_data.data_repo

        result = data_repo.get_data(identifier)
        
        src_id = identifier.find_source()
        src_metadata = data_repo.get_metadata(src_id)

        readable_period = src_metadata["readable_period"]

        # Make sure the directory holding these results is there
        analysis_dir_path = os.path.join(self.out_path, f"{readable_period} analysis")
        if(not os.path.exists(analysis_dir_path)):
            os.mkdir(analysis_dir_path)

        if(src_id not in self.text_results.keys()):
            self.text_results[src_id] = []

        # Save result to CSV if it's a dataframe, save to text file otherwise
        if(isinstance(result, pd.DataFrame)):
            path = os.path.join(analysis_dir_path, f"{identifier.analysis}.csv")
            print(f"  Saving analysis file \"{path}\"")
            result.to_csv(path, index=False)
        else:
            self.text_results[src_id].append(f"{identifier.analysis}: {str(result)}")

    def save_meta_analysis(self, identifier):
        data_repo: DataRepository = self.prog_data.data_repo

        result = data_repo.get_data(identifier)
        
        readable_period = "Entire range"

        # Make sure the directory holding these results is there
        analysis_dir_path = os.path.join(self.out_path, f"{readable_period} analysis")
        if(not os.path.exists(analysis_dir_path)):
            os.mkdir(analysis_dir_path)

        # Save as a DataFrame, meta analyses are always DataFrames
        path = os.path.join(analysis_dir_path, f"{identifier.analysis}.csv")
        print(f"  Saving analysis file \"{path}\"")
        result.to_csv(path, index=False)
                
            
            