from src.data.identifiers.identifier import *

def filter_type(filtertype: type, strict=False):
    """
    Get a list of identifiers that are the same type as the provided type argument.

    Args:
        filtertype (type): The type that the identifier must have.
    Returns:
        Callable[[Identifier], bool]: The lambda operation.
    Raises:
        ValueError: type is not a subclass of Identifier.    
    """

    if(not issubclass(filtertype, Identifier)):
        raise ValueError(f"Cannot filter by type \"{filtertype}\" it is not an instance of Identifier.")

    if strict:
        return lambda identifier: type(identifier) is filtertype
    else:
        return lambda identifier: isinstance(identifier, filtertype)

def filter_source_type(resource_type: str):
    """
    Get a list of SourceIdentifiers that have the same type as resource_type.

    Args:
        resource_type (str): The target resource type for SourceIdentifiers.
    Returns:
        Callable[[Identifier], bool]: The lambda operation.
    """

    analysis_type_lambda = filter_type(SourceIdentifier)
    return lambda identifier: analysis_type_lambda(identifier) and identifier.type == resource_type

def filter_timestamps(start_ts: int, end_ts: int):
    """
    Get a list of TimestampIdentifiers that have the same starting and ending timestamps.
    
    Args:
        start_ts (int): The target start timestamp.
        end_ts (int): The target end timestamp.
    Returns:
        Callable[[Identifier], bool]: The lambda operation.
    """
    analysis_type_lambda = filter_type(TimeStampIdentifier)
    return lambda identifier: analysis_type_lambda(identifier) and identifier.start_ts == start_ts and identifier.end_ts == end_ts

def filter_analyis_type(analysis_type: str):
    """
    Get a list of AnalysisIdentifiers that have the same analysis type.

    Args:
        targ_identifier (Identifier): The identifier that the analyses are performed on.
    Returns:
        Callable[[Identifier], bool]: The lambda operation.
    """

    analysis_type_lambda = filter_type(AnalysisIdentifier)
    return lambda identifier: analysis_type_lambda(identifier) and identifier.analysis == analysis_type

def filter_analyses_of(targ_identifier: Identifier):
    """
    Get a list of AnalysisIdentifiers that are analyses of the provided identifier.

    Args:
        targ_identifier (Identifier): The identifier that the analyses are performed on.
    Returns:
        Callable[[Identifier], bool]: The lambda operation.
    """

    analysis_type_lambda = filter_type(AnalysisIdentifier)
    return lambda identifier: analysis_type_lambda(identifier) and identifier.on == targ_identifier
