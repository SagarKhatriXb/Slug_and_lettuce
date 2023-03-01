import datetime
import hashlib
import json
import os

import pymongo
import scrapy
from scrapy.cmdline import execute


class ThirdSpiderSpider(scrapy.Spider):
    name = 'third_spider'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        mongoDB = pymongo.MongoClient('localhost', 27017)
        db = mongoDB['slug_and_lettuce']
        # self.fatch_table = db[f'data2_{datetime.datetime.today().strftime("%d_%m_%Y")}']
        self.fatch_table = db[f'data2_{datetime.datetime.today().strftime("%d_%m_%Y")}']
        self.insert_table = db[f'data3_{datetime.datetime.today().strftime("%d_%m_%Y")}']
        self.insert_table.create_index("hashid", unique=True)

    def start_requests(self):
        url = "https://hybrid-ordering-api.orderbee.co.uk/menu-pages"


        records = self.fatch_table.find({'status': None})
        # records = self.fatch_table.find({"hashid": 8257372307254470})

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
            image = data['image']
            menu_id = data['menu_id']
            menu_name = data['menu_name']

            payload = json.dumps({
                "siteId": int(zonalSiteId),
                "brandToken": "c2x1ZyZsZXR0dWNlX29yZGVyYmVlOnhZbUkwVWl4",
                "bundleIdentifier": "ob.native.slugandlettuce",
                "salesAreaId": int(salesAreaId),
                "userAgent": "Orderbee - Stonegate",
                "menuId": int(menu_id)
            })
            headers = {
                'accept': 'application/json, text/plain, */*',
                'connection': 'Keep-Alive',
                'content-type': 'application/json',
                'host': 'hybrid-ordering-api.orderbee.co.uk',
                'user-agent': 'okhttp/4.9.1',
                'x-session-id': 'l5FmlUC2LYTtg2EZnDJgK'
            }

            yield scrapy.Request(method="POST", url=url, headers=headers, body=payload, meta={
            'pub_name':pub_name,
            'siteId':siteId,
            'zonalSiteId':zonalSiteId,
            'salesAreaId':salesAreaId,
            'Description':Description,
            'Phone':Phone,
            'addressLine1':addressLine1,
            'addressLine2':addressLine2,
            'town':town,
            'county':county,
            'postcode':postcode,
            'longitude':longitude,
            'latitude':latitude,
            'email':email,
            'image':image,
            'menu_id':menu_id,
            'menu_name':menu_name
            })

    def parse(self, response, **k):
        res = response.text
        print()

        # item = SlugLettuceItem()

        pub_name= response.meta['pub_name']
        siteId= response.meta['siteId']
        zonalSiteId= response.meta['zonalSiteId']
        salesAreaId= response.meta['salesAreaId']
        Description= response.meta['Description']
        Phone= response.meta['Phone']
        addressLine1= response.meta['addressLine1']
        addressLine2= response.meta['addressLine2']
        town= response.meta['town']
        county= response.meta['county']
        postcode= response.meta['postcode']
        longitude= response.meta['longitude']
        latitude= response.meta['latitude']
        email= response.meta['email']
        image= response.meta['image']
        menu_id= response.meta['menu_id']
        menu_name= response.meta['menu_name']



        try:
            html_path = f'D:\\\\XByte(Projects)\\\\Slug_and_lettuce\\\\pagesave\\\\third_spider\\\\{datetime.datetime.today().strftime("%d%m%Y")}'

            if not os.path.exists(html_path):
                os.makedirs(html_path)

            html_file_name = f"{html_path}\\\\{zonalSiteId}_{salesAreaId}_{menu_id}.html"
            with open(html_file_name, 'w', encoding='utf-8') as f:
                f.write(response.text)
                f.close()
        except Exception as e:
            html_file_name = ''
            print(e)

        try:
            json_data = json.loads(response.text)
        except:
            json_data = ''

        if json_data != '':

            for data in json_data['menu']:

                sub_Menu_name = data['groupName']

                for info in data['items']:
                    item = dict()

                    if info['itemType'] == 'product':
                        try:
                            productID = info['productId']
                        except Exception as E:
                            print(E)
                            productID = ''
                        try:
                            productName = info['displayName']
                        except Exception as E:
                            print(E)
                            productName = ''
                        try:
                            displayRecordId = info['displayRecordId']
                        except Exception as E:
                            print(E)
                            displayRecordId = ''
                        try:
                            produrct_description = info['description']
                        except Exception as E:
                            print(E)
                            produrct_description = ''

                        for port in info['portions']:
                            try:
                                portion = port['name']
                            except Exception as E:
                                print(E)
                                portion = ''

                            try:
                                portion_name = port['portion_name']
                            except Exception as E:
                                print(E)
                                portion_name = ''
                            try:
                                portion_price = port['price']
                            except Exception as E:
                                print(E)
                                portion_price = ''


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
                            item['sub_Menu_name'] = sub_Menu_name
                            item['productID'] = productID
                            item['productName'] = productName
                            item['displayRecordId'] = displayRecordId
                            item['produrct_description'] = produrct_description
                            item['portion_name'] = portion_name
                            item['portion'] = portion
                            item['portion_price'] = portion_price
                            item['page_save'] = html_file_name
                            item['time_stamp'] = datetime.datetime.now()

                            hash_utf8 = (
                                f"{item['sub_Menu_name']}{item['portion']}{item['productID']}{item['productName']}{item['displayRecordId']}{item['produrct_description']}{item['portion_name']}{item['portion_price']}{item['pub_name']}{item['siteId']}{item['zonalSiteId']}{item['salesAreaId']}{item['Description']}{item['Phone']}{item['addressLine1']}{item['addressLine2']}{item['town']}{item['county']}{item['postcode']}{item['longitude']}{item['latitude']}{item['email']}{item['image']}{item['menu_id']}{item['menu_name']}").encode(
                                'utf8')
                            Hash_id = int(hashlib.md5(hash_utf8).hexdigest(), 16) % (10 ** 16)
                            item['hashid'] = Hash_id

                            # print(item)
                            try:
                                self.insert_table.insert_one(dict(item))
                                print("Data Inserted", self.insert_table)
                            except Exception as E:
                                print(E)

            try:
                self.fatch_table.update_one({'zonalSiteId': zonalSiteId, 'salesAreaId': salesAreaId, 'menu_id':menu_id},
                                            {"$set": {"status": "done"}})

                print(f"==Updated== > zonalSiteId : {zonalSiteId} & salesAreaId : {salesAreaId}")
            except Exception as E:
                print(E)
        else:
            try:
                self.fatch_table.update_one(
                    {'zonalSiteId': zonalSiteId, 'salesAreaId': salesAreaId, 'menu_id': menu_id},
                    {"$set": {"status": "Json Decode Error"}})

                print(f"==Json Decode Error== > zonalSiteId : {zonalSiteId} & salesAreaId : {salesAreaId}")
            except Exception as E:
                print(E)


if __name__ == '__main__':
    execute("scrapy crawl third_spider".split())

'''




'''
