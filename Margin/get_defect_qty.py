#Good, Done
from yola_functions import getToken, get_dataframe_metabase,toGoogleSheets,fromGoogleSheets
import datetime
from datetime import timedelta
import requests
import pandas as pd
import sys
from io import StringIO
from openpyxl.utils import get_column_letter

############################### Data From Meta API #######################################
token = getToken("ayyoub.mahraz@yolafresh.com","AyyoubMh98@","Metabase")
# Get today's date and yesterday's date
today_date = (datetime.date.today()-timedelta(days=2)).strftime('%Y-%m-%d')

# Construct payload to retrieve quantity data
payload = f'[{{"type":"date/all-options","value":"{today_date}~{today_date}","id":"56c20fe6","target":["dimension",[\"field\","moroco",{{"base-type":"type/DateTime"}}]]}},{{"type":"string/=","value":["2000001","2000002"],"id":"726d156","target":["dimension",[\"field\",18126,null]]}},{{"type":"string/=","id":"424d87f4","target":["dimension",[\"field\",17384,{{"join-alias":"Material Master"}}]]}},{{"type":"string/=","id":"ee7431f5","target":["dimension",[\"field\",17353,{{"join-alias":"Material Master"}}]]}},{{"type":"string/=","id":"f0f48e33","target":["dimension",[\"field\",17534,{{"join-alias":"Invoice Sto Get Details"}}]]}},{{"type":"string/=","id":"33f4cf01","target":["dimension",[\"field\",17345,{{"join-alias":"Material Master"}}]]}}]'
data = get_dataframe_metabase(payload,367,3764,3014,token)
# Rename columns to match the dump section format
# Filter the DataFrame
filtered_data = data[data['Material Master → Material Desc'].str.contains('defect', case=False)]

filtered_data = filtered_data.rename(columns={'Invoice Sto Get Details → Material': 'Material ID', 'Sum of Weight': 'Defect Qty'})
# Print the filtered DataFrame
print(filtered_data)


############################### Data Material Journey #######################################

from datetime import datetime
dict_sections = {
    'Defect (Kg)' : {'range_start_index' : 804, 'range_end_index' : 978, 'column_name' : False},
}
mois_abreges = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

SAMPLE_SPREADSHEET_ID_MaterialJourney = '1YJvykupxFI-7x-BMIyRXg5YXjgbzU5fxarPOp3bMhcw'
sheet_name_MaterialJourney = 'Daily PnL'

date_debut = (datetime.now() + timedelta(hours=0) - timedelta(days=0))
date_format_today = f"{date_debut.day}-{mois_abreges[date_debut.month]}"

# Read data from the material journey dump section
range_start_index = dict_sections['Defect (Kg)']['range_start_index']
range_end_index = dict_sections['Defect (Kg)']['range_end_index']

df_MaterialJourney = fromGoogleSheets(SAMPLE_SPREADSHEET_ID_MaterialJourney, SHEET_NAME=sheet_name_MaterialJourney+'!A1:AC')
print(df_MaterialJourney)

date_format_today = f"{date_debut.day}-{mois_abreges[date_debut.month]}"
day_letter = get_column_letter(df_MaterialJourney.columns.get_loc(date_format_today) + 1)
print(day_letter)


item_range_letter = 'A'
# Extract the specified range from the DataFrame
df_material_journey_dump = df_MaterialJourney.iloc[range_start_index-1 :range_end_index-1]

df_material_journey_dump.rename(columns={'Items' : 'Material ID'}, inplace=True)
print(df_material_journey_dump['Material ID'])
print("=="*30)


df_result_merged = df_material_journey_dump.merge(filtered_data, on='Material ID', how='left')
df_result_merged.fillna(0, inplace=True)

rangeClear=day_letter+str(range_start_index+1)+":"+day_letter+str(range_end_index)
rangeUpdate=day_letter+str(range_start_index+1)

toGoogleSheets(df_result_merged[['Defect Qty']], SAMPLE_SPREADSHEET_ID_MaterialJourney, sheet_name_MaterialJourney, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])
print(df_result_merged)