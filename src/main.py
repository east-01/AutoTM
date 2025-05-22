import os
import sys
import pandas as pd

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from src.program_data.program_data import load_std_prog_data
from data.ingest.ingest import ingest
from src.analysis.analysis import analyze
from src.visualization.visualizations import vizualize
from src.data.processors import *
from src.data.saving.analysis_saver import AnalysisSaver
from src.data.saving.summary_saver import SummarySaver
from src.data.saving.dataframe_saver import DataFrameSaver
from src.data.saving.vis_saver import VizualizationsSaver
from src.data.summary.summarizer import can_summarize, summarize, print_all_summaries

# Hides warnings for .fillna() calls
pd.set_option('future.no_silent_downcasting', True)

prog_data = load_std_prog_data()
print(f"Will perform analyses: {", ".join(prog_data.args.analysis_options)}")
print("")

# Load DataFrames
print("Starting ingest...")
prog_data.data_repo = ingest(prog_data)
prog_data.data_repo = process_periods(prog_data.data_repo)
prog_data.data_repo = generate_metadata(prog_data.data_repo, prog_data.config)

if(has_overlaps(prog_data.data_repo)):
    print("Error: The ingested DataRepository has overlapping timestamps for some of its SourceData. This is not allowed- if using FileSystem ingest try PromQL instead.")

if(prog_data.args.verbose):
    prog_data.data_repo.print_contents()
print("")

# Analyze dataframes
print("Starting analysis...")
analyze(prog_data)

# Summarize analysis results
if(can_summarize(prog_data)):
    summarize(prog_data)
print("")

# Visualize analysis results
print("Generating visualizations...")
vizualize(prog_data)
if(prog_data.args.verbose):
    prog_data.data_repo.print_contents()
print("")

# Save output data, only if an out directory is specified
out_dir = prog_data.args.outdir
if(out_dir is not None):
    print("Saving data...")    
    if(not os.path.exists(out_dir)):
        os.mkdir(out_dir)

    for saver in [DataFrameSaver(prog_data), AnalysisSaver(prog_data), SummarySaver(prog_data), VizualizationsSaver(prog_data)]:
        saver.save()
    print("")

# Print summaries
print_all_summaries(prog_data.data_repo)

try:
    import psutil
    psutil_available = True
except ImportError:
    psutil_available = False

if(psutil_available):
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    print(f"\nMemory usage: {memory_info.rss / (1024 * 1024):.2f} MB")