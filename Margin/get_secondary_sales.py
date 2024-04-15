from yola_functions import getToken, get_dataframe_metabase,toGoogleSheets,fromGoogleSheets
import datetime
from datetime import timedelta
import requests
import pandas as pd
import sys
from io import StringIO
from openpyxl.utils import get_column_letter

############################### Data From Meta API #######################################
day_numb = 1
token = getToken("ayyoub.mahraz@yolafresh.com","AyyoubMh98@","Metabase")
# Get today's date and yesterday's date
today_date = (datetime.date.today()-timedelta(days=day_numb)).strftime('%Y-%m-%d')

# Construct payload to retrieve quantity data
payload = f'[{{"type":"date/all-options","value":"{today_date}~{today_date}","id":"56c20fe6","target":["dimension",[\"field\","moroco",{{"base-type":"type/DateTime"}}]]}},{{"type":"string/=","value":["2000001","2000002"],"id":"726d156","target":["dimension",[\"field\",18126,null]]}},{{"type":"string/=","id":"424d87f4","target":["dimension",[\"field\",17384,{{"join-alias":"Material Master"}}]]}},{{"type":"string/=","id":"ee7431f5","target":["dimension",[\"field\",17353,{{"join-alias":"Material Master"}}]]}},{{"type":"string/=","id":"f0f48e33","target":["dimension",[\"field\",17534,{{"join-alias":"Invoice Sto Get Details"}}]]}},{{"type":"string/=","id":"33f4cf01","target":["dimension",[\"field\",17345,{{"join-alias":"Material Master"}}]]}}]'
data = get_dataframe_metabase(payload,367,3764,3014,token)

# Rename columns to match the dump section format
# Filter the DataFrame
filtered_data = data[data['Material Master → Material Desc'].str.contains('defect', case=False)]

filtered_data = filtered_data.rename(columns={'Invoice Sto Get Details → Material': 'Material ID', 'Sum of Weight': 'Defect Qty'})
# Print the filtered DataFrame
print(filtered_data)
print("**"*40)

############################### Data Sub Category #######################################
SAMPLE_SPREADSHEET_ID_SUBCAT = '1Lp-1iKyZ1MS4ji4HVkDLCV-b6plnpuLLBZWWDOrg1_c'
sheet_name_sub = 'Category_Separation_Script'
df_SubCategory = fromGoogleSheets(SAMPLE_SPREADSHEET_ID_SUBCAT, SHEET_NAME=sheet_name_sub+'!A1:AC')
filtered_Sub_defect = df_SubCategory[df_SubCategory['Material name'].str.contains('defect', case=False)]

df_sub_merged = filtered_data.merge(filtered_Sub_defect, on='Material ID', how='left')
df_sub_merged.fillna(0, inplace=True)
print("Data merged")
#now i have a data merged contains the material id, 
print(df_sub_merged)


############################### Data Gross Margin #######################################

from datetime import datetime
dict_sections = {
    'Secondary Sales (kg)' : {'range_start_index' : 100, 'range_end_index' : 110, 'column_name' : False},
    'Secondary Sales (value)' : {'range_start_index' : 116, 'range_end_index' : 126, 'column_name' : False},
}
mois_abreges = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

SAMPLE_SPREADSHEET_ID_GROSS = '1MNXK0C_tMz71aA7Z9V0eBZmF-i3VAWz2fqjSPDbGPXQ'
sheet_name_gross = 'Daily Aggregated PnL'

date_debut = (datetime.now() + timedelta(hours=0) - timedelta(days=day_numb))
date_format_today = f"{date_debut.day}-{mois_abreges[date_debut.month]}"

range_start_index = dict_sections['Secondary Sales (kg)']['range_start_index']
range_end_index = dict_sections['Secondary Sales (kg)']['range_end_index']

range_start_index_value = dict_sections['Secondary Sales (value)']['range_start_index']
range_end_index_value = dict_sections['Secondary Sales (value)']['range_end_index']

df_GrossMargin = fromGoogleSheets(SAMPLE_SPREADSHEET_ID_GROSS, SHEET_NAME=sheet_name_gross+'!A1:EK')
print("gross Margin data")
print(df_GrossMargin.columns)

date_format_today = f"{date_debut.day}-{mois_abreges[date_debut.month]}"
day_letter = get_column_letter(df_GrossMargin.columns.get_loc(date_format_today) + 1)
print(day_letter)

item_range_letter = 'A'
# Extract the specified range from the DataFrame
df_gross_margin_defect = df_GrossMargin.iloc[range_start_index-1 :range_end_index-1]
df_gross_margin_defect_value = df_GrossMargin.iloc[range_start_index_value-1 :range_end_index_value-1]
print(df_gross_margin_defect)

df_gross_margin_defect.rename(columns={'Overall Company' : 'Sub_category'}, inplace=True)
df_gross_margin_defect_value.rename(columns={'Overall Company' : 'Sub_category'}, inplace=True)
print(df_gross_margin_defect['Sub_category'])
print("=="*30)
# Calculer la somme de Defect Qty groupée par Sub_category
sum_defect_qty_by_subcategory = df_sub_merged.groupby('Sub_category')['Defect Qty'].sum()

sum_value_by_subcategory = df_sub_merged.groupby('Sub_category')['Sum of Invoice Sto Get Details → Total Amount'].sum()

# Afficher le résultat
print(sum_defect_qty_by_subcategory)
print(sum_value_by_subcategory)


df_result_merged_qty = df_gross_margin_defect.merge(sum_defect_qty_by_subcategory, on='Sub_category', how='left')
df_result_merged_value = df_gross_margin_defect_value.merge(sum_value_by_subcategory, on='Sub_category', how='left')
df_result_merged_qty.fillna(0, inplace=True)
df_result_merged_value.fillna(0, inplace=True)
print(df_result_merged_qty)

rangeClear=day_letter+str(range_start_index+1)+":"+day_letter+str(range_end_index)
rangeUpdate=day_letter+str(range_start_index+1)

rangeClearValue=day_letter+str(range_start_index_value+1)+":"+day_letter+str(range_end_index_value)
rangeUpdateValue=day_letter+str(range_start_index_value+1)

toGoogleSheets(df_result_merged_qty[['Defect Qty']], SAMPLE_SPREADSHEET_ID_GROSS, sheet_name_gross, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])
toGoogleSheets(df_result_merged_value[['Sum of Invoice Sto Get Details → Total Amount']], SAMPLE_SPREADSHEET_ID_GROSS, sheet_name_gross, rangeClearValue, rangeUpdateValue, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])
print(df_result_merged_qty)