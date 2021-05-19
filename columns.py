"""
Idea:
    To create a set of models representing the columns inside a CSV. This enables us to be agnostic to the method of
    configuration. i.e. this information could be populated from a JSON configuration file or from a fluent API,
    but we no longer have as great a degree of coupling between the info.json configuration file and the output format.
    Some degree of coupling is bound to remain, but this approach gives us greater flexibility.

    It should be expressive enough that we can generate both CSV-W (QB) and the CMD metadata formats from this
    information. Basically this means that each column definition is a union of the properties required for the CSV-QB
    & CMD formats. I think going forward if and when we add new output formats, we'll need to review these models to
    ensure they're still fit-for-purpose and don't become the new locality of confusing mess.

    Validation of these models should be performed by the CSV-QB *or* CMD metadata file output function. Whether the
    metadata provided is valid depends on whether the model contains the information required by a given output method.
"""

from abc import ABC
from typing import Optional, List, Union, Dict, Any


class CsvColumn(ABC):
    csv_column_title: str

    def __init__(self, csv_column_title: str):
        self.csv_column_title = csv_column_title


class DimensionColumn(CsvColumn):
    parent_dimension_uri: Optional[str]
    dimension_uri: Optional[str]
    value_uri: Optional[str]
    source_uri: Optional[str]

    description: Optional[str]
    label: Optional[str]

    codelist: Optional[Union[str, bool]]

    cmd_associated_notations: Optional[Dict[str, str]]

    def __init__(self,
                 csv_column_title: str,
                 parent_dimension_uri: Optional[str],
                 dimension_uri: Optional[str],
                 value_uri: Optional[str],
                 source_uri: Optional[str],
                 description: Optional[str],
                 label: Optional[str],
                 codelist: Optional[Union[str, bool]]
                 ):
        CsvColumn.__init__(self, csv_column_title)
        self.parent_dimension_uri = parent_dimension_uri
        self.dimension_uri = dimension_uri
        self.value_uri = value_uri
        self.source_uri = source_uri
        self.description = description
        self.label = label
        self.codelist = codelist

    @staticmethod
    def existing_dimension(csv_column_title: str,
                           dimension_uri: str,
                           value_uri: Optional[str]):
        return DimensionColumn(csv_column_title, None, dimension_uri, value_uri, None, None, None, None)

    @staticmethod
    def new_dimension(csv_column_title: str,
                      parent_dimension_uri: Optional[str],
                      value_uri: Optional[str],
                      source_uri: Optional[str],
                      description: Optional[str],
                      label: Optional[str],
                      codelist: Optional[Union[str, bool]]):
        return DimensionColumn(csv_column_title, parent_dimension_uri, None, value_uri, source_uri, description, label,
                               codelist)


class MeasureTypeColumn(DimensionColumn):
    types: Optional[List[str]]

    def get_measure_uri(self) -> str:
        return self.value_uri

    measure_uri: str = property(get_measure_uri)

    @staticmethod
    def existing_measure(csv_column_title: str,
                         dimension_uri: str,
                         measure_uri: str,
                         types: Optional[List[str]]):
        return MeasureTypeColumn(csv_column_title, None, dimension_uri, measure_uri, None, None, None, None, types)

    def __init__(self,
                 csv_column_title: str,
                 parent_dimension_uri: Optional[str],
                 dimension_uri: Optional[str],
                 value_uri: Optional[str],
                 source_uri: Optional[str],
                 description: Optional[str],
                 label: Optional[str],
                 codelist: Optional[Union[str, bool]],
                 types: Optional[List[str]]
                 ):
        DimensionColumn.__init__(self, csv_column_title, parent_dimension_uri, dimension_uri, value_uri, source_uri,
                                 description, label, codelist)
        self.types = types


class ObservedValueColumn(CsvColumn):
    measure_uri: Optional[str]
    value_uri: str

    datatype: Optional[str]

    def __init__(self,
                 csv_column_title: str,
                 measure_uri: Optional[str],
                 unit_uri: Optional[str],
                 datatype: Optional[str]):
        CsvColumn.__init__(self, csv_column_title)
        self.measure_uri = measure_uri
        self.unit_uri = unit_uri
        self.datatype = datatype


class AttributeColumn(CsvColumn):
    attribute_uri: str
    value_uri: str

    def __init__(self, csv_column_title: str, attribute_uri: str, value_uri: str):
        CsvColumn.__init__(self, csv_column_title)
        self.attribute_uri = attribute_uri
        self.value_uri = value_uri

    @staticmethod
    def existing_attribute(csv_column_title: str, attribute_uri: str, value_uri: str):
        return AttributeColumn(csv_column_title, attribute_uri, value_uri)


class SuppressedColumn(CsvColumn):
    def __init__(self, csv_column_title: str):
        CsvColumn.__init__(self, csv_column_title)


def _get_column_for_metadata_config(col_name: str, col_config: Any) -> CsvColumn:
    if isinstance(col_config, dict):
        dimension = col_config.get("dimension")
        value = col_config.get("value")
        parent = col_config.get("parent")
        description = col_config.get("description")
        label = col_config.get("label")
        attribute = col_config.get("attribute")
        unit = col_config.get("unit")
        measure = col_config.get("measure")
        datatype = col_config.get("datatype")

        if dimension is not None and value is not None:
            if dimension == "http://purl.org/linked-data/cube#measureType":
                return MeasureTypeColumn.existing_measure(col_name, dimension, value, col_config.get("types"))
            else:
                return DimensionColumn.existing_dimension(col_name, dimension, value)
        elif parent is not None or description is not None or label is not None:
            return DimensionColumn.new_dimension(col_name, parent, value, col_config.get("source"), description, label,
                                                 col_config.get("codelist"))
        elif attribute is not None and value is not None:
            return AttributeColumn.existing_attribute(col_name, attribute, value)
        elif (unit is not None and measure is not None) or datatype is not None:
            return ObservedValueColumn(col_name, measure, unit, datatype)
        else:
            raise Exception(f"Unmatched column definition: {col_config}")
    elif isinstance(col_config, bool) and not col_config:
        return SuppressedColumn(col_name)
    else:
        # If not defined, treat it as a dimension.
        return DimensionColumn.new_dimension(col_name, None, None, None, None, None, None)


def columns_from_info_json(column_mappings: Dict[str, Any]) -> Dict[str, CsvColumn]:
    csv_columns: Dict[str, CsvColumn] = {}
    for col_name, mapping in column_mappings.items():
        csv_columns[col_name] = _get_column_for_metadata_config(col_name, mapping)

    _set_observation_measure_uri_if_none(csv_columns)

    return csv_columns


def _set_observation_measure_uri_if_none(csv_columns):
    observed_value_columns: List[ObservedValueColumn] = \
        [c for c in csv_columns.values() if isinstance(c, ObservedValueColumn)]
    if len(observed_value_columns) != 1:
        raise Exception(f"Found {len(observed_value_columns)} observation value columns. Expected 1.")
    observed_value_column = observed_value_columns[0]

    if observed_value_column.measure_uri is None:
        measure_type_columns = [c for c in csv_columns.values() if isinstance(c, MeasureTypeColumn)]
        if len(measure_type_columns) != 1:
            raise Exception(f"Found {len(measure_type_columns)} measure type columns. Expected 1.")
        measure_type_column = measure_type_columns[0]

        observed_value_column.measure_uri = measure_type_column.value_uri
