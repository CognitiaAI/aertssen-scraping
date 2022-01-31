import os
import numpy as np
import pandas as pd
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Testing the counts of downloaded material')
    parser.add_argument('--save_path', required=True, help='Path where downloaded material is present')
    parser.add_argument('--excel_path', required=True, help='Path of the excel file')
    parser.add_argument('--interested_cols', required=True, help='Columns to test i.e. Documents etc')
    args = parser.parse_args()
    all_files = os.listdir(args.save_path)
    df = pd.read_excel(args.excel_path)
    interested_cols = args.interested_cols
    all_cols = list(df.columns)
    cols_to_use = []
    col_counter = 1
    for i in range(0, len(all_cols)):
        col_name = interested_cols + str(col_counter)
        if col_name in all_cols:
            cols_to_use.append(col_name)
            col_counter += 1

    filtered_df = df[df[interested_cols + '1'].notna()].reset_index(drop=True)
    df_names = []
    for i in range(0, len(filtered_df)):
        for col in cols_to_use:
            if filtered_df[col].iloc[i] is not np.nan and filtered_df[col].iloc[i] != '':
                df_names.append(filtered_df[col].iloc[i])
    if len(np.unique(df_names)) == len(all_files):
        print(interested_cols + "Test Passed :)", len(np.unique(df_names)), len(all_files))
    else:
        print(interested_cols + "Test Failed :(", len(np.unique(df_names)), len(all_files))
        print(np.unique(df_names))
