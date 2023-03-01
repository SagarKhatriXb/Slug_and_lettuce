# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import datetime

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from slug_lettuce.items import SlugLettuceItem
import pymongo

class SlugLettucePipeline:
    def process_item(self, item, spider):
        if isinstance(item, SlugLettuceItem):
            port = '27017'
            host = 'localhost'
            con = pymongo.MongoClient(f'mongodb://{host}:{port}/')

            mydb = con['slug_and_lettuce']

            conn = mydb[f'data3_{datetime.datetime.today().strftime("%d_%m_%Y")}_main']

            conn.insert_one(dict(item))

            print("insert", conn)
            return item
