import pandas as pd

from src.data.identifiers.identifier import Identifier, SummaryIdentifier
from src.data.filters import *

class DataRepository():
    """
    The DataRepository can hold any data, along with optional metadata; both identified by a 
      string identifier. You can also retrieve lists of identifiers based off of a filtering
      function with the filter calls.
    """

    def __init__(self):
        self._data = {}
        self._metadata = {}
    
    def add(self, identifier: Identifier, data: object, metadata: dict = None):
        """
        Add data and metadata to the DataRepository with an Identifier, stores data and metadata
          in their respective dictionaries using the Identifier.
          
        Args:
            identifier (Identifier): The identifier for the data and metadata.
            data (object): The data to add
            metadata (dict): The metadata to add
        Returns:
            tuple[object, dict]: The data/metadata tuple.
        Raises:
            ValueError: The identifier is already in the repository, the metadata is None.
        """
        
        if(metadata is None):
            metadata = {}

        if(not issubclass(type(identifier), Identifier)):
            raise ValueError(f"Cannot add data for \"{identifier}\" identifier type \"{type(identifier)}\" is not a subclass of Identifier.")
        if(self.contains(identifier)):
            raise ValueError(f"Cannot add data for \"{identifier}\" it already exists in the repo.\nCurrent repo:\n  {"\n  ".join(str(key) for key in self._data.keys())}")
        if(metadata is None):
            raise ValueError(f"Metadata cannot be none.")

        self._data[identifier] = data
        self._metadata[identifier] = metadata

    def update_metadata(self, identifier: Identifier, metadata):
        """
        Update the metadata for a specific identifier.
          
        Args:
            identifier (Identifier): The identifier for the data and metadata.
            metadata (dict): The metadata to update

        Raises:
            ValueError: The identifier is already in the repository, the metadata is None.
        """
        if(not self.contains(identifier)):
            raise ValueError(f"Cannot update metadata for \"{identifier}\" it is not in the repo.")

        self._metadata[identifier] = metadata

    def remove(self, identifier: Identifier):
        """
        Remove the corresponding data and metadata from the repository.
          
        Args:
            identifier (Identifier): The identifier for the data and metadata.
        Raises:
            ValueError: The identifier is not in the repository.
        """
        if(not self.contains(identifier)):
            raise ValueError(f"Cannot remove data for \"{identifier}\" it is not in the repo.")

        self._data.pop(identifier)
        if(identifier in self._metadata):
            self._metadata.pop(identifier)

    def contains(self, identifier: Identifier) -> bool:
        """
        Check if the identifier is in the DataRepository.

        Args:
            identifier (Identifier): The identifier for the data and metadata.
        Returns:
            bool: Contains status.
        """
        return identifier in self._data.keys()

    def get(self, identifier: Identifier) -> tuple[object, dict]:
        """
        Get the corresponding data object and metadata dictionary as a tuple. Retrieving both in
          one call. Is a shortcut for the get_data & get_metadata calls.
          
        Args:
            identifier (Identifier): The identifier for the data and metadata.
        Returns:
            tuple[object, dict]: The data/metadata tuple.
        Raises:
            KeyError: The identifier is not in the repository.
        """
        if(not self.contains(identifier)):
            raise KeyError(f"Cannot get data/metadata for \"{identifier}\" it is not in the repo.")

        return (self.get_data(identifier), self.get_metadata(identifier))

    def get_data(self, identifier: Identifier) -> object:
        """
        Get the corresponding data object.
          
        Args:
            identifier (Identifier): The identifier for the data and metadata.
        Returns:
            object: The data object.
        Raises:
            KeyError: The identifier is not in the repository.
        """
        if(not self.contains(identifier)):
            raise KeyError(f"Cannot get data for \"{identifier}\" it is not in the repo.")

        return self._data[identifier]

    def get_metadata(self, identifier: Identifier) -> dict:
        """
        Get the corresponding metadata dictionary.
          
        Args:
            identifier (Identifier): The identifier for the data and metadata.
        Returns:
            dict: The metadata dictionary.
        Raises:
            KeyError: The identifier is not in the repository.
        """
        if(not self.contains(identifier)):
            raise KeyError(f"Cannot get metadata for \"{identifier}\" it is not in the repo.")
        if(identifier not in self._metadata.keys()):
            self._metadata[identifier] = {}

        return self._metadata[identifier]

    def get_ids(self):
        """
        Get the list of identifiers in the DataRepository.

        Returns:
            list[Identifier]: The list of identifiers.
        """
        return self._data.keys()

    def filter_ids(self, operation = lambda identifier: True) -> list:
        """
        Get a list of identifiers that satisfy an operation. The operation must return true/false.

        Args:
            operation (function): The operation to apply to each identifier.
        Returns:
            list[Identifier]: The list of identifiers that satisfy the operation.
        Raises:
            ValueError: Operation is none.    
        """
        if(operation is None):
            raise ValueError("Operation cannot be None.")

        out_list = []
        for identifier in self._data.keys():
            if(operation(identifier)):
                out_list.append(identifier)

        return out_list

    def count(self):
        return len(self._data.keys())
    
    def join(self, other_repo):
        """
        Joins another DataRepository into this one. Raises an error if any identifier
        from the other repository already exists in this repository.
        
        Args:
            other_repo (DataRepository): The repository to join into this one.
        
        Raises:
            ValueError: If there is an identifier collision.
        """
        if not isinstance(other_repo, DataRepository):
            raise TypeError("Expected other_repo to be an instance of DataRepository.")

        overlapping_ids = [id_ for id_ in other_repo.get_ids() if self.contains(id_)]
        if overlapping_ids:
            overlap_str = "\n  ".join(str(id_) for id_ in overlapping_ids)
            raise ValueError(f"Cannot join repositories. The following identifiers already exist:\n  {overlap_str}")
        
        for id_ in other_repo.get_ids():
            data, metadata = other_repo.get(id_)
            self.add(id_, data, metadata)

    def print_contents(self, include_metadata=False):
        print("Summary of DataRepository:")
        for identifier in self.get_ids():
            data = self.get_data(identifier)
            datastr = ""
            if(isinstance(data, pd.DataFrame)):
                datastr = "DataFrame"
            elif(isinstance(identifier, SummaryIdentifier)):
                datastr = "Summary tuple"
            else:
                datastr = str(data)

            outstr = f"ID {identifier}: \n  {"\n  ".join(datastr.split("\n"))}"
            if(include_metadata):
                metadata = self.get_metadata(identifier)
                outstr += f"\n  {"\n  ".join(str(metadata).split("\n"))}"

            print(outstr)
