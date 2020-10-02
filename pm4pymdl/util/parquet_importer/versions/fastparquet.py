from fastparquet import ParquetFile
from pm4py.objects.log.util import dataframe_utils
import deprecation
from pm4pydistr.util.parquet_importer.parameters import Parameters
from pm4py.util import exec_utils

COLUMNS = Parameters.COLUMNS.value


def apply(path, parameters=None):
    """
    Import a Parquet file

    Parameters
    -------------
    path
        Path of the file to import
    parameters
        Parameters of the algorithm, possible values:
            Parameters.COLUMNS -> columns to import from the Parquet file

    Returns
    -------------
    df
        Pandas dataframe
    """
    if parameters is None:
        parameters = {}

    columns = exec_utils.get_param_value(Parameters.COLUMNS, parameters, None)

    pf = ParquetFile(path)

    if columns:
        df = pf.to_pandas(columns)
    else:
        df = pf.to_pandas()

    return dataframe_utils.legacy_parquet_support(df)
