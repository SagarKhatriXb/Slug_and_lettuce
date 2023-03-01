import hashlib
import json
import os
import scrapy
from scrapy.cmdline import execute
import pymongo
import datetime


class SecondSpiderSpider(scrapy.Spider):
    name = 'second_spider'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        mongoDB = pymongo.MongoClient('localhost', 27017)
        db = mongoDB['slug_and_lettuce']
        self.fatch_table = db[f'data1_{datetime.datetime.today().strftime("%d_%m_%Y")}']
        self.insert_table = db[f'data2_{datetime.datetime.today().strftime("%d_%m_%Y")}']
        self.insert_table.create_index("hashid", unique=True)

    def start_requests(self):
        url = "https://hybrid-ordering-api.orderbee.co.uk/menus"

        records = self.fatch_table.find({"status": None})

        for data in records:
            pub_name = data['pub_name']
            siteId = data['siteId']
            zonalSiteId = data['zonalSiteId']
            salesAreaId = data['salesAreaId']
            Description = data['Description']
            Phone = data['Phone']
            addressLine1 = data['addressLine1']
            addressLine2 = data['addressLine2']
            town = data['town']
            county = data['county']
            postcode = data['postcode']
            longitude = data['longitude']
            latitude = data['latitude']
            email = data['email']



            payload = json.dumps({
                "siteId": int(zonalSiteId),
                "brandToken": "c2x1ZyZsZXR0dWNlX29yZGVyYmVlOnhZbUkwVWl4",
                "bundleIdentifier": "ob.native.slugandlettuce",
                "userAgent": "Orderbee - Stonegate",
                "salesAreaId": int(salesAreaId)
            })

            # payload = json.dumps({
            #     "siteId": int(601),
            #     "brandToken": "c2x1ZyZsZXR0dWNlX29yZGVyYmVlOnhZbUkwVWl4",
            #     "bundleIdentifier": "ob.native.slugandlettuce",
            #     "userAgent": "Orderbee - Stonegate",
            #     "salesAreaId": int(588)
            # })

            headers = {
                'accept': 'application/json, text/plain, */*',
                'connection': 'Keep-Alive',
                'content-type': 'application/json',
                'host': 'hybrid-ordering-api.orderbee.co.uk',
                'user-agent': 'okhttp/4.9.1',
                'x-session-id': 'l5FmlUC2LYTtg2EZnDJgK'
            }

            yield scrapy.Request(method="POST", url=url, headers=headers, body=payload, meta={
                'pub_name': pub_name,
                'siteId': siteId,
                'zonalSiteId': zonalSiteId,
                'salesAreaId': salesAreaId,
                'Description': Description,
                'Phone': Phone,
                'addressLine1': addressLine1,
                'addressLine2': addressLine2,
                'town': town,
                'county': county,
                'postcode': postcode,
                'longitude': longitude,
                'latitude': latitude,
                'email': email

            }, callback=self.parse)

    def parse(self, response, **k):
        # response = response.text

        pub_name = response.meta['pub_name']
        siteId = response.meta['siteId']
        zonalSiteId = response.meta['zonalSiteId']
        salesAreaId = response.meta['salesAreaId']
        Description = response.meta['Description']
        Phone = response.meta['Phone']
        addressLine1 = response.meta['addressLine1']
        addressLine2 = response.meta['addressLine2']
        town = response.meta['town']
        county = response.meta['county']
        postcode = response.meta['postcode']
        longitude = response.meta['longitude']
        latitude = response.meta['latitude']
        email = response.meta['email']

        try:
            html_path = f'D:\\\\XByte(Projects)\\\\Slug_and_lettuce\\\\pagesave\\\\second_spider\\\\{datetime.datetime.today().strftime("%d%m%Y")}'

            if not os.path.exists(html_path):
                os.makedirs(html_path)

            data_html_file_name = f"{html_path}\\\\{zonalSiteId}_{salesAreaId}.html"
            with open(data_html_file_name, 'w', encoding='utf-8') as f:
                f.write(response.text)
                f.close()
        except Exception as e:
            data_html_file_name = ''
            print(e)

        json_data = json.loads(response.text)

        for info in json_data['menus']:
            item = dict()

            try:
                image = info['image']
            except Exception as E:
                print(E)
                image = ''
            try:
                menu_id = info['id']
            except Exception as E:
                print(E)
                menu_id = ''
            try:
                menu_name = info['name']
            except Exception as E:
                print(E)
                menu_name = ''

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
            item['image'] = image
            item['menu_id'] = menu_id
            item['menu_name'] = menu_name

            hash_utf8 = (
                f"{item['pub_name']}{item['siteId']}{item['zonalSiteId']}{item['salesAreaId']}{item['Description']}{item['Phone']}{item['addressLine1']}{item['addressLine2']}{item['town']}{item['county']}{item['postcode']}{item['longitude']}{item['latitude']}{item['email']}{item['image']}{item['menu_id']}{item['menu_name']}").encode(
                'utf8')
            Hash_id = int(hashlib.md5(hash_utf8).hexdigest(), 16) % (10 ** 16)
            item['hashid'] = Hash_id

            try:
                self.insert_table.insert_one(dict(item))
                print("Data Inserted", self.insert_table)
            except Exception as E:
                print(E)

        try:
            self.fatch_table.update_one({'zonalSiteId': zonalSiteId, 'salesAreaId': salesAreaId},
                                        {"$set": {"status": "done"}})

            print(f"======Updated===============",)
        except Exception as E:
            print(E)


if __name__ == '__main__':
    execute("scrapy crawl second_spider".split())
