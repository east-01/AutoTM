import calendar

from src.program_data.program_data import ProgramData
from src.data.data_repository import DataRepository
from src.data.identifiers.identifier import *
from src.data.filters import *
from src.utils.timeutils import get_range_as_month

class VisualizationVariables():
    """
    A class that takes in a list of variables and a DataRepository, resolving each variable name to
      its corresponding value in the DataRepository.
    Convert strings with variable placeholders into resolved variables.
    For example, if we want to resolve the string:
      Total CPU Hours: %TOTCPUHRS%
    We can use the VisualizationVariables class to turn it into:
      Total CPU Hours: 212.2
    """

    def __init__(self, prog_data: ProgramData, identifier: AnalysisIdentifier, variables: dict):
        data_repo: DataRepository = prog_data.data_repo

        # Holds the variable name and parsed variable value
        self.parsed_variables = {}
        for variable_name in variables:
            # The analysis name that the variable wants to load
            targ_analysis = variables[variable_name]
            
            # Resolve the corresponding analysis variable with matching SourceIdentifier
            variable_value = None
            for comp_id in data_repo.filter_ids(filter_analyis_type(targ_analysis)):
                if(comp_id.find_source() == identifier.find_source()):
                    variable_value = data_repo.get_data(comp_id)

            if(variable_value is None):
                raise Exception(f"Failed to resolve corresponding analysis variable for {targ_analysis}, current SourceID: {identifier.find_source()}")

            if(variable_value is None):
                variable_value = variable_name
            else:
                self.parsed_variables[variable_name] = variable_value

        src_id: SourceIdentifier = identifier.find_source()
        range_data = get_range_as_month(src_id.start_ts, src_id.end_ts, prog_data.config['step'])

        self.parsed_variables["MONTH"] = calendar.month_name[range_data["month"]]
        self.parsed_variables["YEAR"] = range_data["year"]

    def apply_variables(self, text):
        for variable_name in self.parsed_variables:
            text = text.replace(f"%{variable_name}%", str(self.parsed_variables[variable_name]))
        return text