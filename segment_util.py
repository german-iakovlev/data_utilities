import os
import analytics
import pandas as pd
import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials

os.chdir('needed_directory')

SHEET_URL = 'sheets_url'

SEGMENT_WRITE_KEY = 'segment_write_key.txt'
GDRIVE_API_CREDENTIALS = 'gd_api_key.json'
USER_ID = '#####'

# Segment write key
with open(SEGMENT_WRITE_KEY) as key_file:
    segment_key = key_file.readline()

analytics.write_key = segment_key

# Google Drive API credentials
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(
    GDRIVE_API_CREDENTIALS, scope
)

gc = gspread.authorize(credentials)

def sheet_to_dataframe(sheet_url):
    '''
    Fetches all values from the google drive sheet
    and converts the data to data frame
    '''
    sheet = gc.open_by_url(sheet_url).sheet1
    sheet_data = sheet.get_all_values()
    columns = sheet_data[0]
    values = sheet_data[1:]
    df = pd.DataFrame(values, columns=columns)
    return df


def dataframe_to_segment(dataframe):

    for row in dataframe.iterrows():
        analytics.track(USER_ID,'Acquisition Forecast', {
            'month': row[1]['month'],
            'leads': row[1]['leads']
        })


clean_data = sheet_to_dataframe(SHEET_URL)
dataframe_to_segment(clean_data)
