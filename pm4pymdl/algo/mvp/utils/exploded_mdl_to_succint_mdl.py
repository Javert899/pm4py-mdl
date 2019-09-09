from pm4pymdl.algo.mvp.utils import exploded_mdl_to_stream
import pandas as pd


def apply(df):
    stream = exploded_mdl_to_stream.apply(df)

    df = pd.DataFrame(stream)
    df.type = "succint"

    return df
