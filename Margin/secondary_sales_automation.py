#done, cogs, secondary sales value and kg, still just the problem in friday (secondary sales value to check).
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

today_date = (datetime.date.today()-timedelta(days=0)).strftime('%Y-%m-%d')

# Construct payload to retrieve quantity data
payload = f'[{{"type":"date/all-options","value":"{today_date}~{today_date}","id":"56c20fe6","target":["dimension",[\"field\","moroco",{{"base-type":"type/DateTime"}}]]}},{{"type":"string/=","value":["2000001","2000002"],"id":"726d156","target":["dimension",[\"field\",18126,null]]}},{{"type":"string/=","id":"424d87f4","target":["dimension",[\"field\",17384,{{"join-alias":"Material Master"}}]]}},{{"type":"string/=","id":"ee7431f5","target":["dimension",[\"field\",17353,{{"join-alias":"Material Master"}}]]}},{{"type":"string/=","id":"f0f48e33","target":["dimension",[\"field\",17534,{{"join-alias":"Invoice Sto Get Details"}}]]}},{{"type":"string/=","id":"33f4cf01","target":["dimension",[\"field\",17345,{{"join-alias":"Material Master"}}]]}}]'
data = get_dataframe_metabase(payload,367,3764,3014,token)

# Rename columns to match the dump section format
# Filter the DataFrame
filtered_data = data[data['Material Master → Material Desc'].str.lower().str.contains('defect', case=False)]

filtered_data = filtered_data.rename(columns={'Invoice Sto Get Details → Material': 'Material ID', 'Sum of Weight': 'Defect Qty'})
filtered_data['Defect Qty'] = filtered_data['Defect Qty'].astype(float)

############################### Data Sub Category #######################################
SAMPLE_SPREADSHEET_ID_SUBCAT = '1Lp-1iKyZ1MS4ji4HVkDLCV-b6plnpuLLBZWWDOrg1_c'
sheet_name_sub = 'Category_Separation_Script'
df_SubCategory = fromGoogleSheets(SAMPLE_SPREADSHEET_ID_SUBCAT, SHEET_NAME=sheet_name_sub+'!A1:AC')
filtered_Sub_defect = df_SubCategory[df_SubCategory['Material name'].str.lower().str.contains('defect', case=False)]

df_sub_merged = filtered_data.merge(filtered_Sub_defect, on='Material ID', how='left')
df_sub_merged.fillna(0, inplace=True)
df_sub_merged = df_sub_merged.rename(columns={'Material ID': 'Defect_ID'})

#now i have a data merged(defect data) contains the date,material id, defect qty, subcategory


############################### Data: material id and the defect id #######################################

SAMPLE_SPREADSHEET_ID_CONCAT = '18OHURdDbTxt7SHux0Lr971rfLn07D9BNOfsv-eSRlZQ'
sheet_name_concat = 'concatenation'
df_Concatenation = fromGoogleSheets(SAMPLE_SPREADSHEET_ID_CONCAT, SHEET_NAME=sheet_name_concat+'!A1:AC')
df_Concatenation = df_Concatenation[['Defect_ID','Material_ID','Material_Name']]


#now i have the defect data with its id when it's not defect
df_result_merged_Ids = df_sub_merged.merge(df_Concatenation, on='Defect_ID', how='left')

############################### Data: From the Pre GrossMargin #######################################

SAMPLE_SPREADSHEET_ID_PRE_GROSS = '1nFxBvNv9Ejz_dECse65eZ1B6bUBtBb2WXh6V2xmEGLw'
sheet_name_pregross = 'Par_Sku'
df_PreGrossMargin = fromGoogleSheets(SAMPLE_SPREADSHEET_ID_PRE_GROSS, SHEET_NAME=sheet_name_pregross+'!A1:AC')
df_PreGrossMargin = df_PreGrossMargin[['Material ID','Prix d\'achat']]
df_PreGrossMargin = df_PreGrossMargin.rename(columns={'Material ID': 'Material_ID'})

df_result_merged_prix_Achat = df_result_merged_Ids.merge(df_PreGrossMargin, on='Material_ID', how='left')

# Assuming df_result_merged_prix_Achat is your DataFrame

# Convert 'Prix d'achat' column to numeric type
df_result_merged_prix_Achat['Prix d\'achat'] = pd.to_numeric(df_result_merged_prix_Achat['Prix d\'achat'], errors='coerce')

# Convert 'Defect Qty' column to numeric type
df_result_merged_prix_Achat['Defect Qty'] = pd.to_numeric(df_result_merged_prix_Achat['Defect Qty'], errors='coerce')

# Calculate COGS (Cost of Goods Sold)
df_result_merged_prix_Achat['COGS'] = df_result_merged_prix_Achat['Prix d\'achat'] * df_result_merged_prix_Achat['Defect Qty']

df_result_merged_prix_Achat_groupe_sub = df_result_merged_prix_Achat.groupby('Sub_category')['COGS', 'Defect Qty', 'Sum of Invoice Sto Get Details → Total Amount'].sum()

############################### Data Gross Margin #######################################

from datetime import datetime
dict_sections = {
    'Secondary Sales COGS' : {'range_start_index' : 132, 'range_end_index' : 142, 'column_name' : False},
    'Secondary Sales (kg)' : {'range_start_index' : 100, 'range_end_index' : 110, 'column_name' : False},
    'Secondary Sales (value)' : {'range_start_index' : 116, 'range_end_index' : 126, 'column_name' : False}

}
mois_abreges = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

SAMPLE_SPREADSHEET_ID_GROSS = '1MZY4v-pk-LrIo68Bn_dopuS4trWlTC2DTfMxXx927sg'
sheet_name_gross = 'Daily Aggregated PnL'

date_debut = (datetime.now() + timedelta(hours=0) - timedelta(days=0))
date_format_today = f"{date_debut.day}-{mois_abreges[date_debut.month]}"

df_GrossMargin = fromGoogleSheets(SAMPLE_SPREADSHEET_ID_GROSS, SHEET_NAME=sheet_name_gross+'!A1:EK')

df_GrossMargin.rename(columns={'Overall Company' : 'Sub_category'}, inplace=True)

item_range_letter = 'A'

day_letter = get_column_letter(df_GrossMargin.columns.get_loc(date_format_today) + 1)
print(day_letter)

range_start_index = dict_sections['Secondary Sales COGS']['range_start_index']
range_end_index = dict_sections['Secondary Sales COGS']['range_end_index']

df_gross_margin_COGS = df_GrossMargin.iloc[range_start_index-1 :range_end_index-1]
df_result_merged_cogs = df_gross_margin_COGS.merge(df_result_merged_prix_Achat_groupe_sub, on='Sub_category', how='left')
df_result_merged_cogs.fillna(0, inplace=True)

rangeClear=day_letter+str(range_start_index+1)+":"+day_letter+str(range_end_index)
rangeUpdate=day_letter+str(range_start_index+1)

toGoogleSheets(df_result_merged_cogs[['COGS']], SAMPLE_SPREADSHEET_ID_GROSS, sheet_name_gross, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])
#=========================================================
range_start_index = dict_sections['Secondary Sales (kg)']['range_start_index']
range_end_index = dict_sections['Secondary Sales (kg)']['range_end_index']

rangeClear=day_letter+str(range_start_index+1)+":"+day_letter+str(range_end_index)
rangeUpdate=day_letter+str(range_start_index+1)

toGoogleSheets(df_result_merged_cogs[['Defect Qty']], SAMPLE_SPREADSHEET_ID_GROSS, sheet_name_gross, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])
#=========================================================
range_start_index = dict_sections['Secondary Sales (value)']['range_start_index']
range_end_index = dict_sections['Secondary Sales (value)']['range_end_index']

rangeClear=day_letter+str(range_start_index+1)+":"+day_letter+str(range_end_index)
rangeUpdate=day_letter+str(range_start_index+1)

toGoogleSheets(df_result_merged_cogs[['Sum of Invoice Sto Get Details → Total Amount']], SAMPLE_SPREADSHEET_ID_GROSS, sheet_name_gross, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])
#=========================================================

