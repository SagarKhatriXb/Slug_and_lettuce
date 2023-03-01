import datetime
import json
import requests
from scrapy.http import HtmlResponse
import pandas as pd


def bau_data(project_id, userstory_id, file_name):
    url = "https://dev.xbytedev.co/count_dashboard/ajax.php?action=add_update_temp_taiga_project_count_data"

    df_from_file = pd.read_excel(file_name)

    dash_total_count = str(len(df_from_file))
    dash_fields_count = df_from_file.count(axis=0).to_dict()
    dash_fields_count = json.dumps(dash_fields_count)

    dash_data = {
        'project_id': project_id,
        'userstory_id': userstory_id,
        'scraped': 1,
        'delivered': 1,
        'total_count': dash_total_count,
        'field_count': dash_fields_count
    }

    try:
        dash_res = requests.post(url, data=dash_data)
        dash_response = HtmlResponse(url=dash_res.url, body=dash_res.content)
        print(dash_response.text)

    except Exception as e:
        print(e)


if __name__ == '__main__':

    file_name = f"\\\\192.168.100.249\\DataGators\\Temp\\Sagar.K\\oxford\\St Austell\\st_austell_{datetime.date.today().strftime('%Y%m%d')}.xlsx"
    project_id = 53
    userstory_id = 641

    bau_data(project_id, userstory_id, file_name)
