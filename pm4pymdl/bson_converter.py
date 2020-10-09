import bson
import json
import pandas as pd
import os


def convert(source_path, target_path):
    from pm4pymdl.util.parquet_exporter import exporter as parquet_exporter
    limit_length = 35
    codec_options = bson.CodecOptions(unicode_decode_error_handler='ignore')
    gen = bson.decode_file_iter(open(source_path, 'rb'), codec_options=codec_options)
    json_list = []
    df_list = []
    i = 0
    for row in gen:
        cols = list(row.keys())
        for col in cols:
            if type(row[col]) is dict:
                if "id" in row[col]:
                    row[col] = row[col]["id"]
                else:
                    del row[col]
            elif type(row[col]) is str and len(row[col]) > limit_length:
                del row[col]
        json_list.append(row)
        if i > 0 and i % 10000 == 0:
            print(i)
            df = pd.DataFrame(json_list)
            df_list.append(df)
            json_list = None
            json_list = []
        i = i + 1
    if json_list:
        df = pd.DataFrame(json_list)
        df_list.append(df)
    if df_list:
        overall_df = pd.concat(df_list)
        for col in overall_df.columns:
            overall_df[col] = overall_df[col].astype(str)
        parquet_exporter.apply(overall_df, target_path, parameters={"compression": "gzip"})


folder_content = os.listdir("msr14")
for file in folder_content:
    if file.endswith(".bson") and not file == "commits.bson":
        print("converting ",file)
        source_path = os.path.join("msr14", file)
        target_path = os.path.join("output", file)
        convert(source_path, target_path)
