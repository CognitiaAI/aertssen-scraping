import os

import scrapy
import pandas as pd
import numpy as np
from scrapy.http import Request


class DocdownloaderSpider(scrapy.Spider):
    name = 'docdownloader'
    print("Doc Downloader Constructor Called !!!")
    final_df = pd.read_excel('./scrapy_aertssen.xlsx')
    docs_df = final_df[final_df['Documents 1'].notna()].reset_index(drop=True)
    print("DOCS shape: ", docs_df.shape)
    print("Done reading")
    DOC_SAVE_DIR = './documents/'
    all_documents = os.listdir((DOC_SAVE_DIR))

    def start_requests(self):
        yield scrapy.Request(url='https://www.google.com/', callback=self.parse_main_page,
                             dont_filter=True)

    def save_pdf(self, response):
        name = response.meta['name']
        self.logger.info('Saving PDF %s', name)
        with open(name, 'wb') as file:
            file.write(response.body)

    def parse_main_page(self, response):
        total_length = len(self.docs_df)
        for i in range(0, total_length):
            for k in range(1, 3):
                doc_name = self.docs_df['Documents ' + str(k)].iloc[i]
                doc_link = self.docs_df['Documents Link ' + str(k)].iloc[i]
                if doc_name is not np.nan and doc_name != '':
                    try:
                        length = len(doc_name)
                        if doc_name not in self.all_documents:
                            print("Doing: ", doc_name, " ", i)
                            yield Request(
                                url=doc_link, callback=self.save_pdf, meta={'name': self.DOC_SAVE_DIR + doc_name}
                            )
                    except:
                        pass
