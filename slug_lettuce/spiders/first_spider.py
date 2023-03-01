import json
import datetime
import os
import pymongo
import scrapy
from scrapy.cmdline import execute


class FirstSpiderSpider(scrapy.Spider):
    name = 'first_spider'

    def __init__(self, **kwargs):

        mongoDB = pymongo.MongoClient('localhost',27017)
        db = mongoDB['slug_and_lettuce']
        self.table = db[f'data1_{datetime.datetime.today().strftime("%d_%m_%Y")}']

    def start_requests(self):
        url = "https://cdn.contentful.com/spaces/gzhrube5q6fz/environments/live/entries?content_type=brand&include=10&limit=1000"

        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip',
            'authorization': 'Bearer du26JWHnCqzP1dvB7uHpQ3o1beDtyQBq5ELaxShZFLk',
            'connection': 'Keep-Alive',
            'host': 'cdn.contentful.com',
            'if-none-match': 'W/"12025964342322018212"',
            'user-agent': 'okhttp/4.9.1',
            'x-contentful-user-agent': 'sdk contentful.js/9.1.6; platform ReactNative;'
        }

        yield scrapy.Request(url=url, headers=headers, callback=self.parse)

    def parse(self, response, **k):
        # a = str(response.text).strip()

        try:
            html_path = f'D:\\\\XByte(Projects)\\\\Slug_and_lettuce\\\\pagesave\\\\first_spider\\\\{datetime.datetime.today().strftime("%d%m%Y")}'
            # D:\X-Byte(Projects)\easy_order\easy order
            if not os.path.exists(html_path):
                os.makedirs(html_path)

            data_html_file_name = f"{html_path}\\\\pub_names.html"
            with open(data_html_file_name, 'w', encoding='utf-8') as f:
                f.write(response.text)
                f.close()
        except Exception as e:
            data_html_file_name = ''
            print(e)

        json_data = json.loads(response.text)

        for info in json_data['includes']['Entry']:

            try:
                orderAtTable = info['fields']['orderAtTable']
            except Exception as E:
                print(E)
                orderAtTable = ''


            if orderAtTable != '':
                item = dict()

                try:
                    pub_name = info['fields']['name']
                except Exception as E:
                    pub_name = ''
                    print(E)
                try:
                    siteId = info['fields']['siteId']
                except Exception as E:
                    siteId = ''
                    print(E)
                try:
                    zonalSiteId = info['fields']['zonalSiteId']
                except Exception as E:
                    zonalSiteId = ''
                    print(E)
                try:
                    salesAreaId = info['fields']['salesAreaId']
                except Exception as E:
                    salesAreaId = ''
                    print(E)
                try:
                    Description = info['fields']['siteDescription']
                except Exception as E:
                    Description = ''
                    print(E)
                try:
                    Phone = info['fields']['sitePhoneNumber']
                except Exception as E:
                    Phone = ''
                    print(E)
                try:
                    addressLine1 = info['fields']['addressLine1']
                except Exception as E:
                    addressLine1 = ''
                    print(E)
                try:
                    addressLine2 = info['fields']['addressLine2']
                except Exception as E:
                    addressLine2 = ''
                    print(E)
                try:
                    town = info['fields']['town']
                except Exception as E:
                    town = ''
                    print(E)
                try:
                    county = info['fields']['county']
                except Exception as E:
                    county = ''
                    print(E)
                try:
                    postcode = info['fields']['postcode']
                except Exception as E:
                    postcode = ''
                    print(E)
                try:
                    longitude = info['fields']['location']['lon']
                except Exception as E:
                    longitude = ''
                    print(E)
                try:
                    latitude = info['fields']['location']['lat']
                except Exception as E:
                    latitude = ''
                    print(E)

                try:
                    email = info['fields']['siteEmail']
                except Exception as E:
                    email = ''
                    print(E)

                item['pub_name'] = pub_name
                item['siteId'] = siteId
                item['zonalSiteId'] = zonalSiteId
                item['salesAreaId'] = salesAreaId
                item['Description'] = Description
                item['Phone'] = Phone
                item['addressLine1'] = addressLine1
                item['addressLine2'] = addressLine2
                item['town'] = town
                item['county'] = county
                item['postcode'] = postcode
                item['longitude'] = longitude
                item['latitude'] = latitude
                item['email'] = email


                try:
                    self.table.insert_one(dict(item))
                    print("Data Inserted..", self.table)
                except Exception as E:
                    print(E)


if __name__ == '__main__':
    execute("scrapy crawl first_spider".split())


