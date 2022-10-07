import csv
import json
import logging
import pathlib
from collections import namedtuple
from typing import Dict, List, Union

from pipeline.settings import db_connection

Technology = namedtuple("Technology", ["technology", "technology_type"])
EnergyVectors = namedtuple("EnergyVectors", ["input", "output"])


PARAMETER_MODEL_SCALAR_COLUMNS = {
    "id",
    "region",
    "year",
    "bandwidth_type",
    "version",
    "method",
    "source",
    "comment",
}
PARAMETER_MODEL_TIMESERIES_COLUMNS = {
    "id",
    "region",
    "version",
    "method",
    "source",
    "comment",
    "timeindex_start",
    "timeindex_stop",
    "timeindex_resolution",
}
OEDATAMODEL_SCALAR_COLUMNS = {
    "id",
    "parameter_name",
    "technology",
    "unit",
    "technology_type",
    "region",
    "year",
    "bandwidth",
    "bandwidth_type",
    "version",
    "method",
    "source",
    "comment",
}
OEDATAMODEL_TIMESERIES_COLUMNS = {
    "id",
    "parameter_name",
    "technology",
    "unit",
    "technology_type",
    "region",
    "series",
    "timeindex_start",
    "timeindex_stop",
    "timeindex_resolution",
    "version",
    "method",
    "source",
    "comment",
}


class ParameterModelError(Exception):
    """Raised if something is wrong with the ParameterModel"""


class MetadataError(Exception):
    """Raised if something is wrong within metadata"""


class OntologyError(Exception):
    """Raised if something could not be found within OEO"""


def get_ontology_entry(ontology_entry: dict) -> dict:
    """
    Returns OEO entry for given OEO code

    If ontology path (IRI) is not found, name of ontology is returned instead.

    Parameters
    ----------
    ontology_entry: dict
        Ontology entry of metadata (either from subject, isAbout or valueReference)

    Returns
    -------
    dict: Entries of given OEO code
    """
    # TODO: Write real lookup code
    ontology = {
        "http://openenergy-platform.org/ontology/oeo/OEO00000311": {
            "label": "onshore wind farm",
            "isSubClassOf": "wind farm",
        },
        "http://openenergy-platform.org/ontology/oeo/OEO00000165": {
            "label": "field photovoltaic power plant",
            "isSubClassOf": "photovoltaic power plant",
        },
        "http://openenergy-platform.org/ontology/oeo/OEO_00140050": {
            "label": "efficiency value"
        },
    }
    if ontology_entry["path"] not in ontology:
        logging.warning("Could not find ontology concept for '%s'.", ontology_entry)
        return {"label": ontology_entry["name"]}
    return ontology[ontology_entry["path"]]


def get_energy_vectors(technology_type: str, parameter_name: str) -> EnergyVectors:
    """
    Looks up energy vectors for SEDOS Energysystem Flow

    Parameters
    ----------
    technology_type: str
        Technology type build from subject section in metadata
    parameter_name: str
        Parameter name build from isAbout section in metadata

    Returns
    -------
    EnergyVectors
        Input and output energy vector for given parameter

    Raises
    ------
    KeyError
        If energy vector cannot be found in DB
    """
    cur = db_connection.cursor()
    cur.execute(
        """
        SELECT input_vector, output_vector
        FROM energy_vectors
        WHERE
        technology_type = :technology_type AND
        parameter_name = :parameter_name
        """,
        {"technology_type": technology_type, "parameter_name": parameter_name},
    )

    if result := cur.fetchone():
        return EnergyVectors(*result)
    raise KeyError(
        f"Could not find energy vector for '{technology_type:=}' and '{parameter_name:=}'."
    )


def get_technology_and_technology_type(subject: List[Dict[str, str]]) -> Technology:
    """
    Converts subject(s) from OEO into technology and technology type

    If subject consists of two entries, first entry is used as technology and second as technology_type.
    If subject consists of single entry, entry is used as technology_type and entries parent class is used as
    technology.

    Parameters
    ----------
    subject: List[Dict[str, str]]
        Subject read from OEMetadata

    Returns
    -------
    Technology
        Contains technology and technology_type for OEDatamodel

    Raises
    ------
    MetadataError
        if subject cannot be found in OEMetadata
    """
    if not subject:
        raise MetadataError("Subject of metadata is invalid.")
    if len(subject) > 1:
        return Technology(
            get_ontology_entry(subject[0])["label"],
            get_ontology_entry(subject[1])["label"],
        )
    ontology_entry = get_ontology_entry(subject[0])
    # pylint: disable=W0511
    if (
        "isSubClassOf" in ontology_entry
    ):  # FIXME: isSubClassOf most likely is wrong key!
        return Technology(
            ontology_entry["label"],
            ontology_entry["isSubClassOf"],
        )
    logging.warning("No technology concept found for '%s'.", subject)
    return Technology(ontology_entry["label"], ontology_entry["label"])


def get_parameter_name(metadata_field: dict) -> str:
    """
    Converts isAbout from OEO into parameter name

    Parameter name is build by looking up ontology labels from isAbout entries and joining them.
    If no isAbout field is given or isAbout is empty, field name is returned.

    Parameters
    ----------
    metadata_field: dict
        Resource field of OEMetadata

    Returns
    -------
    str
        Parameter name for OEDatamodel
    """
    if "isAbout" not in metadata_field or not metadata_field["isAbout"]:
        return metadata_field["name"]
    return " ".join(
        get_ontology_entry(about)["label"] for about in metadata_field["isAbout"]
    )


def get_parameter_field(metadata_schema: dict, parameter: str) -> dict:
    """
    Returns field from metadata schema for given parameter

    Parameters
    ----------
    metadata_schema: dict
        Schema from OEMetadata
    parameter: str
        Parameter name from ParameterModel

    Returns
    -------
    dict
        Field from OEMetadata resource schema for corresponding parameter

    Raises
    ------
    MetadataError
        if field for parameter cannot be found in OEMetadata
    """
    try:
        return next(
            field for field in metadata_schema["fields"] if field["name"] == parameter
        )
    except StopIteration:
        # pylint: disable=W0707
        raise MetadataError(
            f"Could not find parameter '{parameter}' in metadata schema."
        )


class OEDatamodelFromParametermodel:
    def __init__(
        self,
        parametermodel_filename: Union[pathlib.Path, str],
        oedatamodel_filename: Union[pathlib.Path, str],
        metadata: dict,
    ):
        """
        Converts parametermodel into OEDatamodel using OEMetadata

        Parameters
        ----------
        parametermodel_filename: Union[pathlib.Path, str]
            Path to load CSV file of parameter model
        oedatamodel_filename: Union[pathlib.Path, str]
            Path to store CSV file of (SEDOS-)OEDatamodel
        metadata: dict
            Metadata of parameter model
        """
        self.parametermodel_filename = parametermodel_filename
        self.oedatamodel_filename = oedatamodel_filename
        self.metadata = metadata
        self.__init_parameters()

    def __init_parameters(self):
        """Initializes global parameters"""
        with open(
            self.parametermodel_filename, "r", encoding="utf-8"
        ) as parametermodel_file:
            parametermodel = csv.DictReader(parametermodel_file, delimiter=";")
            self.is_timeseries = "timeindex_start" in parametermodel.fieldnames
            self.parametermodel_colums = (
                PARAMETER_MODEL_TIMESERIES_COLUMNS
                if self.is_timeseries
                else PARAMETER_MODEL_SCALAR_COLUMNS
            )

            self.parameters = [
                column
                for column in parametermodel.fieldnames
                if column not in self.parametermodel_colums
            ]

        self.parameter_names = {}
        self.parameter_units = {}
        for parameter in self.parameters:
            field = get_parameter_field(
                self.metadata["resources"][0]["schema"], parameter
            )
            self.parameter_names[parameter] = get_parameter_name(field)
            self.parameter_units[parameter] = field["unit"]

        self.technology, self.technology_type = get_technology_and_technology_type(
            self.metadata["subject"]
        )

    def __convert_row(self, row: dict, parameter: str) -> dict:
        """
        Converts one row from ParameterModel into OEDatamodel

        Parameters
        ----------
        row: dict
            Current row from ParameterModel
        parameter: str
            As each row in ParameterModel may contain multiple parameters,
            each parameter is mapped to a row in OEDatamodel

        Returns
        -------
        dict
            Row in OEDatamodel format

        Raises
        ------
        ParameterModelError
            if column cannot be found in ParameterModel
        """

        def read_json_cell(json_entry: str) -> str:
            """
            Reads a cell from ParameterModel and tries to extract information for current parameter

            Parameters
            ----------
            json_entry: str
                Cell from ParameterModel containing a JSON-string

            Returns
            -------
            str
                Output for current cell for given parameter

            Raises
            ------
            ParameterModelError
                if JSON is corrupt or parameter cannot be found in JSON-keys
            """
            try:
                entry_dict = json.loads(json_entry)
            except json.decoder.JSONDecodeError as de:
                raise ParameterModelError(
                    f"ID #{row['id']}: JSON entry for {column:=} seems corrupt."
                ) from de
            try:
                return entry_dict[parameter]
            except KeyError as ke:
                raise ParameterModelError(
                    f"ID #{row['id']}: Could not find entry for {parameter:=} in {column:=}."
                ) from ke

        oedatamodel_row = {
            "technology": self.technology,
            "technology_type": self.technology_type,
            "parameter_name": self.parameter_names[parameter],
            "unit": self.parameter_units[parameter],
        }
        if self.is_timeseries:
            oedatamodel_row["series"] = row[parameter]
        else:
            oedatamodel_row["bandwidth"] = row[parameter]
        for column in self.parametermodel_colums:
            try:
                entry = row[column]
            except KeyError:
                # pylint: disable=W0707
                raise ParameterModelError(f"Parameter model needs {column:=}.")
            oedatamodel_row[column] = read_json_cell(entry) if "{" in entry else entry
        return oedatamodel_row

    def convert(self):
        """
        Converts ParameterModel from given file into OEDatamodel and saves it as CSV
        """
        with open(
            self.parametermodel_filename, "r", encoding="utf-8"
        ) as parametermodel_file, open(
            self.oedatamodel_filename, "w", encoding="utf-8"
        ) as oedatamodel_file:
            fieldnames = (
                OEDATAMODEL_TIMESERIES_COLUMNS
                if self.is_timeseries
                else OEDATAMODEL_SCALAR_COLUMNS
            )
            oedatamodel = csv.DictWriter(oedatamodel_file, fieldnames=fieldnames)
            oedatamodel.writeheader()

            parametermodel = csv.DictReader(parametermodel_file, delimiter=";")
            for row in parametermodel:
                oedatamodel.writerows(
                    self.__convert_row(row, parameter) for parameter in self.parameters
                )
