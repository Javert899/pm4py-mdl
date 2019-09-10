from pm4pymdl.algo.mvp.utils import succint_stream_to_exploded_stream
import pandas as pd


def apply(df):
    stream = df.to_dict('r')

    exploded_stream = succint_stream_to_exploded_stream.apply(stream)

    df = pd.DataFrame(exploded_stream)
    df.type = "exploded"

    return df
