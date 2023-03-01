
import os

import numpy as np
import pymongo
import pandas as pd
import datetime


def export():
    conn = pymongo.MongoClient('localhost', 27017)


    db = conn["slug_and_lettuce"]

    table = f"data3_{datetime.date.today().strftime('%d_%m_%Y')}"

    collection = db[table]
    cursor = collection.find()
    print(cursor.count())

    df = pd.DataFrame(cursor)
    df.pop('_id')
    df.pop('hashid')
    print(df)

    client_file_name = f"slug_and_lettuce_{datetime.date.today().strftime('%Y%m%d')}"

    export_list = ['\\\\192.168.100.249\\DataGators\\Temp\\Sagar.K\\oxford\\slug_and_lettuce', 'D:\\\\XByte(Projects)\\\\Slug_and_lettuce\\\\export_file']

    # client_file = f"D:\\\\XByte(Projects)\\\\Slug_and_lettuce\\\\export_file"
    for export_path in export_list:
        if not os.path.exists(export_path):
            os.makedirs(export_path)


        writer = pd.ExcelWriter(f'{export_path}\\\\{client_file_name}.xlsx', engine='xlsxwriter', engine_kwargs=dict(
            options={'strings_to_urls': False}))
        df.to_excel(writer, 'data', index=False)

        workbook = writer.book
        worksheet = writer.sheets['data']

        # Add a header format.

        header_format = workbook.add_format({
            'bold': True,
            # 'text_wrap': True,
            'valign': 'top',
            'fg_color': '#C0C0C0',
            'border': 1})

        # comment: Write the column headers with the defined format.

        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        format1 = workbook.add_format({'num_format': '#,##0'})
        for i in range(len(df.columns) + 1):
            worksheet.set_column(i, i, 15, format1)

        writer.save()
        print("Excel Genrated")


if __name__ == '__main__':
    export()





