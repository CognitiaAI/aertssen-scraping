import scrapy
import unicodedata
from pydispatch import dispatcher
from scrapy import signals
import pandas as pd
from scrapy.loader import ItemLoader


class ScrapyAertssenSpider(scrapy.Spider):
    name = 'scrapy_aertssen'
    start_urls = ['https://www.aertssentrading.com/aertssentrading/search.aspx']
    pre_url = 'https://www.aertssentrading.com'
    page_pre = 'https://www.aertssentrading.com/aertssentrading/search.aspx?q=&Page='
    page_post = '&PageSize=12&SortBy=createdate_desc'
    final_df = pd.DataFrame()
    total_pages = 12
    counter = 0

    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        self.final_df.to_excel('./scrapy_aertssen.xlsx', index=False)

    def start_requests(self):
        for i in range(0, self.total_pages):
            page_link = self.page_pre + str(i + 1) + self.page_post
            yield scrapy.Request(url=page_link, callback=self.parse_page,
                                 headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                                                        '(KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36'
                                          }, dont_filter=True, meta={'url': page_link})

    def parse_page(self, response):
        links = response.xpath("//table[@class='list list_vertical']/tr/td/span[@class="
                               "'field field_brandmodel']/a/@href").extract()
        link_htmls = response.xpath("//table[@class='list list_vertical']/tr").extract()
        link_elements = response.xpath("//table[@class='list list_vertical']/tr")
        for i in range(0, len(links)):
            abs_link = self.pre_url + links[i]
            pre_dictionary = self.get_pre_dictionary(link_htmls[i], abs_link, link_elements[i])
            yield scrapy.Request(url=abs_link, callback=self.parse_one_machine,
                                 headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                                                        '(KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36'
                                          }, dont_filter=True, meta={'pre_dictionary': pre_dictionary})

    def get_pre_dictionary(self, link_html, abs_link, link_element):
        pre_dictionary = dict()
        pre_dictionary['Link HTML'] = link_html
        pre_dictionary['URL'] = abs_link
        pre_dictionary['Title'] = link_element.xpath("./td/span[@class='field field_brandmodel']"
                                                     "/a/text()").extract_first()
        pre_dictionary['Pre Time'] = link_element.xpath("./td/span[@class='field field_brandmodel']"
                                                    "/span/text()").extract_first()
        availability = link_element.xpath("./td/span[@class='field field_availability "
                                          "statusExpected']/text()").extract_first()
        if availability is None:
            availability = link_element.xpath("./td/span[@class='field field_"
                                              "availability']/text()").extract_first()
        pre_dictionary['Pre Availability'] = availability
        pre_dictionary['Pre Year'] = link_element.xpath("./td/span[@class='field field_yearofmanufacture']"
                                                    "/text()").extract_first()
        pre_dictionary['Pre Hours'] = link_element.xpath("./td/span[@class='field field_meterreadouthours']"
                                                     "/text()").extract_first()
        price = link_element.xpath("./td/span[@class='field field_price']/span/span/text()").extract_first()
        if price is not None:
            price = str(price) + link_element.xpath("./td/span[@class='field field_price']/span/text()").extract_first()
        pre_dictionary['Pre Price'] = price
        pre_dictionary['Pre Condition'] = link_element.xpath(
            "./td/span[@class='field field_antique']/text()").extract_first()
        return pre_dictionary

    def get_dictionary_from_table(self, elements, url):
        inner_dict = dict()
        for i in range(0, len(elements)):
            header = elements[i].xpath("./td[@class='header']/span/text()").extract_first()
            value = elements[i].xpath("./td[@class='cell1']/span/text()").extract_first()
            if header is not None:
                if value is None:
                    if header == 'Documents':
                        hrefs = elements[i].xpath("./td[@class='cell1']/span/table/tr/td/a/@href").extract()
                        names = elements[i].xpath("./td[@class='cell1']/span/table/tr/td/a/text()").extract()
                        if len(names) != len(hrefs):
                            print("[DEBUG: 404!!] Documents names and length not equal", " Counter: ", self.counter,
                                  " URL: ", url)
                        elif len(hrefs) > 0:
                            for k in range(0, len(hrefs)):
                                inner_dict['Documents Link ' + str(k + 1)] = self.pre_url + hrefs[k]
                                inner_dict['Documents ' + str(k + 1)] = names[k]
                        else:
                            print(" [DEBUG: 404!!] Doc 1", header, " : ", url)
                    else:
                        str_value = ''
                        values = elements[i].xpath("./td[@class='cell1']/span/table/tr/td/text()").extract()
                        if len(values) > 0:
                            for val in values:
                                str_value += val + '\n'
                            value = str_value
                        else:
                            print("[DEBUG: 404!!] Doc 2", header, " : ", url)
                        inner_dict[header] = unicodedata.normalize("NFKD", value)
                else:
                    inner_dict[header] = unicodedata.normalize("NFKD", value)
            else:
                print("Parent Header ?.? ", " : ", url)
        return inner_dict

    def get_dictionary_from_price_table(self, elements, url):
        inner_dict = dict()
        for i in range(0, len(elements)):
            header = elements[i].xpath("./td[@class='header']/span/text()").extract_first()
            if header == 'Choose currency':
                continue
            else:
                value = elements[i].xpath("./td[@class='cell1']/span/span/span/text()").extract_first()
                currency = elements[i].xpath("./td[@class='cell1']/span/span/text()").extract_first()
                if value is not None and currency is not None:
                    value += currency
                if value is None:
                    print("[DEBUG: 404!!] Price -> ", url, " : counter : ", self.counter)
                inner_dict[header] = value
        return inner_dict

    def get_summary(self, elements, url):
        inner_dict = dict()
        for i in range(0, len(elements)):
            values = elements[i].xpath("./div/text()").extract()
            if len(values) > 2:
                print("[DEBUG: 404!!] Summary: ", url, " : ", self.counter)
            else:
                inner_dict[values[0]] = values[1]
        return inner_dict

    def get_images_dictionary(self, elements, url):
        inner_dict = dict()
        for i in range(0, len(elements)):
            link_key = 'Image Link ' + str(i + 1)
            name_key = 'Image ' + str(i + 1)
            inner_dict[link_key] = elements[i].xpath('./@src').extract_first()
            name = elements[i].xpath('./@alt').extract_first()
            if name is not None:
                name = name + '-' + str(self.counter) + '-' + str(i + 1)
                name = name.replace(' ', '-').replace(',', '-').replace('.', '-').replace('/', '-')
                name = name.replace('?', '-').replace('\\', '-').replace(':', '-').replace('*', '-')
                name = name.replace('<', '-').replace('>', '-').replace('|', '-').replace('\n', '-')
                for k in range(0, 50):
                    name = name.replace('--', '-')
                if name[0] == '-':
                    name = name[1:]
                name += '.jpg'
            else:
                print(" Image Name is none: ", self.counter, " ", url)
            inner_dict[name_key] = name
        return inner_dict

    def get_seller_dictionary(self, elements, url):
        inner_dict = dict()
        for i in range(0, len(elements)):
            seller_link = elements[i].xpath("./div[@class='contact_name_photo']/img/@src").extract_first()
            if seller_link is not None:
                seller_link = self.pre_url + seller_link
                seller_link = seller_link.replace(' ', '_')
            else:
                print("Seller is None: ", self.counter, " ", url)
            seller_name = elements[i].xpath("./div[@class='contact_name']/text()").extract_first()
            seller_address = elements[i].xpath("./div[@class='contact_address']/text()").extract_first()
            contact_table = elements[i].xpath("./div[@class='contact_data']/table/tr")
            for contact_element in contact_table:
                key = contact_element.xpath("./td[@class='data_label']/text()").extract_first()
                key = key.replace(':', '')
                key = key.replace('Language', 'Contact Language')
                key = key.replace('Telephone', 'Contact')
                key = key.replace('Mobile phone', 'Mobile No.')
                seller_number = contact_element.xpath("./td/a/text()").extract_first()
                if seller_number is None:
                    seller_number = contact_element.xpath("./td[2]/text()").extract_first()
                inner_dict[key + ' ' + str(i + 1)] = seller_number
            inner_dict['Dealer Name' + ' ' + str(i + 1)] = seller_name
            inner_dict['Dealer Logo Link' + ' ' + str(i + 1)] = seller_link
            inner_dict['Dealer Country' + ' ' + str(i + 1)] = seller_address
            if seller_link is not None:
                inner_dict['Dealer Logo' + ' ' + str(i + 1)] = seller_link.split('/')[-1]
        return inner_dict

    def parse_one_machine(self, response):
        final_dictionary = dict()
        pre_dictionary = response.meta['pre_dictionary']
        final_dictionary = dict(**final_dictionary, **pre_dictionary)
        final_dictionary['Specification HTML'] = response.xpath("//div[@id='product_details']").extract_first()
        specification_elements = response.xpath("//table[@class='data data_basic']/tr[@class!='header']")
        specs_dictionary = self.get_dictionary_from_table(specification_elements, pre_dictionary['URL'])
        basic_keys = list(specs_dictionary.keys())
        if 'Machine Location' in basic_keys and 'Country' in basic_keys:
            specs_dictionary['Country Combined'] = specs_dictionary['Machine Location'] + ',' + \
                                                   specs_dictionary['Country']
        final_dictionary = {**final_dictionary, **specs_dictionary}
        detail_elements = response.xpath("//table[@class='data data_details']/tr[@class!='header']")
        detail_dictionary = self.get_dictionary_from_table(detail_elements, pre_dictionary['URL'])
        if 'Price excl. VAT' in detail_dictionary.keys():
            detail_dictionary['Price excl. VAT Spec'] = detail_dictionary.pop('Price excl. VAT')
        final_dictionary = {**final_dictionary, **detail_dictionary}
        price_elements = response.xpath("//table[@class='data data_price']/tr[@class!='header']")
        price_dictionary = self.get_dictionary_from_price_table(price_elements, pre_dictionary['URL'])
        final_dictionary = {**final_dictionary, **price_dictionary}
        summary_elements = response.xpath("//div[@class='product_summary']/div")
        summary_dictionary = self.get_summary(summary_elements, pre_dictionary['URL'])
        final_dictionary = {**final_dictionary, **summary_dictionary}
        images_elements = response.xpath("//*[@id='links']/a/img")
        image_dictionary = self.get_images_dictionary(images_elements, pre_dictionary['URL'])
        final_dictionary = {**final_dictionary, **image_dictionary}
        youtube_links = response.xpath("//iframe[@name='videos']/@src").extract()
        for i in range(0, len(youtube_links)):
            final_dictionary['YouTube ' + str(i + 1)] = youtube_links[i]
        seller_elements = response.xpath("//div[@class='tab_item']")
        seller_dictionary = self.get_seller_dictionary(seller_elements, pre_dictionary['URL'])
        breadcrumbs = response.xpath("//div[@id='path']/a/text()").extract()
        breadcrumbs_str = ''
        for i in range(0, len(breadcrumbs)):
            breadcrumbs_str += breadcrumbs[i]
            if i != len(breadcrumbs) - 1:
                breadcrumbs_str += ' | '
        final_dictionary['Breadcrumbs'] = breadcrumbs_str
        final_dictionary = {**final_dictionary, **seller_dictionary}
        all_keys = list(final_dictionary.keys())
        final_dictionary = {k: [final_dictionary[k]] for k in all_keys}
        row = pd.DataFrame.from_dict(final_dictionary)
        self.final_df = pd.concat([self.final_df, row], axis=0, ignore_index=True)
        self.counter += 1
