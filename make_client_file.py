import os
import numpy as np
import pandas as pd

if __name__ == "__main__":
    sample_path = '/home/faizan/web_scraping/Cognitia/bas-scraping/BAS/Sample_Client.xlsx'
    aertssen_path = '/home/faizan/web_scraping/Cognitia/aertssen/scrapy_aertssen.xlsx'
    sample_df = pd.read_excel(sample_path)
    df = pd.read_excel(aertssen_path)
    mapping = {
    'Pre Condition': 'Condition', 'Price': 'RRP', 'Brand': 'Make', 'Year of manufacture': 'Year', 
    'Hour meter reads': 'Engine Hours', 'Gross Weight': 'Weight', 'Original colour': 'Color',
    'Manufacturing / Serial number': 'Serial', 'Registration year': '1st Registration', 'Documents 1': 'Documents for this vehicle 1',
    'Documents 2': 'Documents for this vehicle 2', 'Contact Languages 1': 'Contact Language 1', 'Contact Languages 2': 'Contact Language 2'
    }
    df.rename(columns=mapping, inplace=True)
    sample_cols = list(sample_df.columns)
    dealer_sample = ['Dealer Name', 'Dealer Country', 'Dealer Logo', 'Contact', 'Mobile No.', 'Contact Email', 'Contact Language']
    specs_cols = sample_cols[:25]
    dealer_cols = []
    for i in range(1, 3):
        for col in dealer_sample:
            dealer_cols.append(col + ' ' + str(i))
    meta_cols = sample_cols[32:34]
    documents_cols = sample_cols[34:48]
    youtube_cols = []
    for i in range(1,6):
        youtube_cols.append('YouTube ' + str(i))
        youtube_cols.append('YouTube Length ' + str(i))
        youtube_cols.append('YouTube Publication ' + str(i))
    youtube_cols.append('3D')
    image_cols = sample_cols[52:102]
    extra_cols = sample_cols[102:]
    arranged_cols = []
    arranged_cols.extend(specs_cols)
    arranged_cols.extend(dealer_cols)
    arranged_cols.extend(documents_cols)
    arranged_cols.extend(youtube_cols)
    arranged_cols.extend(image_cols)
    arranged_cols.extend(extra_cols)
    for i in range(1, 25):
        del df['Image Link ' + str(i)]
    for i in range(1, 3):
        del df['Dealer Logo Link ' + str(i)]
        del df['Documents Link ' + str(i)]
    del df['Link HTML']
    df_cols = list(df.columns)
    arranged_cols.extend(list(set(df_cols) - set(arranged_cols)))
    for col in arranged_cols:
        if col not in df_cols:
            df[col] = ''
    df[arranged_cols].to_excel('/home/faizan/web_scraping/Cognitia/aertssen/Aertssen_31-01-22_dumm.xlsx', index=False)