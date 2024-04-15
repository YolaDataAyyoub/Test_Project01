import pandas as pd
from datetime import datetime
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import PatternFill

from googleapiclient.discovery import build
from google.oauth2 import service_account
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from io import StringIO
import json
import sys
import requests

from datetime import datetime, timedelta

# if datetime.today().strftime('%A') != 'Saturday' :
#     sys.exit()

SERVICE_ACCOUNT_FILE= 'keys.json'
SCOPES=['https://www.googleapis.com/auth/spreadsheets']

SAMPLE_SPREADSHEET_ID = '1lL0f1UvsLrtf8H3ORSdcLo2JuFq5FQroBMRBjjzVvC8'

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name('keys.json', scope)
gc = gspread.authorize(credentials)

def FromGoogleSheets(SAMPLE_SPREADSHEET_ID, SHEET_NAME, SCOPES=['https://www.googleapis.com/auth/spreadsheets']):
        try :
                creds = None
                creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)

                service = build('sheets', 'v4', credentials=creds)
                sheet = service.spreadsheets()
                result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SHEET_NAME).execute()
                values = result.get('values', [])

                if not values:
                    print('Aucune donnée trouvée.')
                else:
                    df = pd.DataFrame(values[1:], columns=values[0])

                    print("Done...")

        except Exception as e :
            return print("Erreur : ", str(e))

        return df

def ToGoogleSheets(DataBase, SAMPLE_SPREADSHEET_ID, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets']):
    try :
            creds = None
            creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)

            service = build('sheets', 'v4', credentials=creds)

            sheet = service.spreadsheets()

            #============== List of Sheet's Names
            ListSheetsNames = [ListSheetsName]

            for SheetName in ListSheetsNames:

                    request = service.spreadsheets().values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SheetName+"!"+rangeClear).execute()
                    Data = DataBase
                    Data.fillna('', inplace=True)
                    Data = cast_for_gsheets(Data)
                    Data = Data.values.tolist()
                    request = service.spreadsheets().values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SheetName+"!"+rangeUpdate, valueInputOption="USER_ENTERED", body={"values":Data}).execute()
                    
    except Exception as e :
            return print('Erreur : ', str(e))

def cast_for_gsheets(df):
    # casting as string if not serializable
    for column, dt in zip(df.columns, df.dtypes):
        if dt.type not in [
            np.int64,
            np.float_,
            np.bool_,
        ]:
            df.loc[:, column] = df[column].astype(str)
    return df

# "2023-10-02"
#=================================================================================================== so itemised report






#==================================================================================================== Préparation de Source -- New Growth battles
# DC1 = 2000001
# DC2 = 2000002

def DFSansParam(code_1, code_2, code_3, date_debut, date_fin, DC) : 

    headers = {
    "Cookie": cookies,
    "Cache-Control": "no-cache",
    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
    }

    url = f"https://insights.censanext.com/api/dashboard/{code_1}/dashcard/{code_2}/card/{code_3}/query/csv"

    if  DC == None :
        data = {
    "parameters":f'[{{"type":"string/=","value":["2000001", "2000002"],"id":"b2e6005a","target":["dimension",["field","plant",{{"base-type":"type/Text"}}]]}},{{"type":"date/all-options","value":"{str(date_debut)}~{str(date_fin)}","id":"99f2e497","target":["dimension",["field","invoice_date",{{"base-type":"type/DateTime"}}]]}}]'    
    }
    else :   

        data = {
    "parameters":f'[{{"type":"string/=","value":["{DC}"],"id":"b2e6005a","target":["dimension",["field","plant",{{"base-type":"type/Text"}}]]}},{{"type":"date/all-options","value":"{str(date_debut)}~{str(date_fin)}","id":"99f2e497","target":["dimension",["field","invoice_date",{{"base-type":"type/DateTime"}}]]}}]'    
        }

    # Effectuez la requête HTTP
    response = requests.post(url, data=data, headers=headers)

    # Assurez-vous que le texte est correctement encodé (en supposant que le texte est en UTF-8)
    response.encoding = 'utf-8'

    return pd.read_csv(StringIO(response.text))


#===================================================================================================================== Login

url = "https://insights.censanext.com/api/session"

payload = json.dumps({
"username": "saad.ounzar@yolafresh.com",
"password": "Asaad123@",
"remember": True
})
headers = {

'Content-Type': 'application/json',

}

response = requests.request("POST", url, headers=headers, data=payload)
print(response.headers.get('Set-Cookie'))
cookies = response.headers.get('Set-Cookie')

# so_item_wow
so_item_wow = {
    "code_1": 518,
    "code_2": 5155,
    "code_3": 4296,
}

#========================================================================================================================================================== CatQuantity ==========================================================================================================================================================

#========================================================================================================================================================== Hyper parametres
DC1 = 2000001
DC2 = 2000002

# Week-1
date_debut = ((datetime.now()+timedelta(hours=0)) - timedelta(days=7)).strftime("%Y-%m-%d")
date_fin = ((datetime.now()+timedelta(hours=0)) - timedelta(days=1)).strftime("%Y-%m-%d")

# Salesmen
Salesmen = {
    "code_1": 290,
    "code_2": 2965,
    "code_3": 2123,
}

# subCat
subCat = {
    "code_1": 290,
    "code_2": 2966,
    "code_3": 2166,
}

# # ASM
# Asm = {
#     "code_1": 290,
#     "code_2": 2981,
#     "code_3": 2127,
# }
#========================================================================================================================================================== Fonction KPI
DC = None
# so_item.loc[-1] = list(so_item.columns)
# print(so_item.columns)
# so_item = so_item.reset_index(drop=True)


# Obtenez la date du premier jour du mois dernier
end_day = datetime.now().replace(day=1) - timedelta(days=1)
first_day = end_day.replace(day=1)

so_item_all = pd.DataFrame()

i=8
for i in range(8,38):
    first_day = first_day + timedelta(days=i)
    so_item = DFSansParam(so_item_wow["code_1"], so_item_wow["code_2"], so_item_wow["code_3"], first_day, first_day, DC)
    so_item_all = pd.concat([so_item_all, so_item])
    print(first_day.strftime("%Y-%m-%d"), first_day.strftime("%Y-%m-%d"))
    print('==============')
    i=1
    if first_day.strftime("%Y-%m-%d") == end_day.strftime("%Y-%m-%d"):
        break




so_item = DFSansParam(so_item_wow["code_1"], so_item_wow["code_2"], so_item_wow["code_3"], date_debut, date_fin, DC)

ListSheetsName = 'SO'
#============== DataBase Section ====================
rangeClear = "A2:AI"
rangeUpdate = "A2"

ToGoogleSheets(so_item, SAMPLE_SPREADSHEET_ID, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])
so_item = FromGoogleSheets(SAMPLE_SPREADSHEET_ID, SHEET_NAME='SO!A1:AI')

so_item['Invoice Date'] = so_item['Invoice Date'].str.slice(0, 10)

so_item['Invoice Date'] = pd.to_datetime(so_item['Invoice Date'])
so_item['Invoice Date'] = so_item['Invoice Date'].dt.strftime("%d-%m-%Y")

# so_item = so_item.groupby(['Material Code', 'Material Name','Material Code', 'Material Code'])['returnqty weight'].sum().reset_index()

#========================================================================================================================================================== Materiel list

#============================= Sauvegarde de fichier Excel ====================================

# URL de l'API
url = "https://userlogin-compass-api.censanext.com/v1/admin/login"

# Données du corps de la demande
data = {
    "email": "asaad.ounzar+00@yolafresh.com",
    "password": "U2FsdGVkX18hmlzruYK3CbsLUm6PTN37VDATcNyGexQ=",
    "companyCode": "20000",
    "portalType": "admin"
}

# Convertir les données en JSON
json_data = json.dumps(data)

# En-têtes de la requête
headers = {
    "Content-Type": "application/json"
}

# Envoyer la requête POST
response = requests.post(url, data=json_data, headers=headers)

# Obtenir la réponse
response_data = response.json()

# Traiter la réponse
if response.status_code == 200:
    # Succès

    access_token = response_data.get("data", {}).get("token")
    print("Connexion réussie. Access Token:", access_token)
    # access_token = response_data.get("access_token")
    # print("Connexion réussie. Access Token:", access_token)
else:
    # Erreur
    error_message = response_data.get("message")
    print("Erreur lors de la connexion:", error_message)

url = 'https://admin-compass-api.censanext.com/api/v1/items/itemListcsv'

payload = {}
payload2 = {}
headers = {
'X-access-token': access_token    
}

response = requests.request("GET", url, headers=headers, data=payload)

text_data = response.text

# Convert text data to a pandas DataFrame
df_listMaterial= pd.DataFrame([line.split(',') for line in text_data.strip().replace('"','').split('\n')], columns=None)

df_listMaterial = df_listMaterial.iloc[:, [0, 1, 3, 4, 8, 10]]

# Supprimer la première ligne
df_listMaterial = df_listMaterial.iloc[1:]

# Réinitialiser les indices du DataFrame
df_listMaterial = df_listMaterial.reset_index(drop=True)

# Renommer les colonnes
df_listMaterial = df_listMaterial.rename(columns={0: 'Material Code', 4: 'Sub_Category'})

df_listMaterial = df_listMaterial[['Material Code', 'Sub_Category']]

#========================================================================================================================================================== Fonction KPI
def DataF(date_debut, date_fin, DC1, DC2, code_1, code_2, code_3) : 

    headers = {
    "Cookie" : cookies,
    "Cache-Control": "no-cache",
    'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
    }

    url = f"https://insights.censanext.com/api/dashboard/{code_1}/dashcard/{code_2}/card/{code_3}/query/csv"

    data = {
    # "parameters": f'[{{"type":"string/=","value":["{DC1}","{DC2}"],"id":"838104f8","target":["dimension",["field",23484,null]]}}]
    "parameters": f'[{{"type":"date/range","value":"{date_debut}~{date_fin}","id":"1a8d7edd","target":["dimension",["field",23496,null]]}},{{"type":"string/=","value":["{DC1}","{DC2}"],"id":"838104f8","target":["dimension",["field",23484,null]]}}]'
    }

    # Effectuez la requête HTTP
    response = requests.post(url, data=data, headers=headers)

    # Assurez-vous que le texte est correctement encodé (en supposant que le texte est en UTF-8)
    response.encoding = 'utf-8'

    return pd.read_csv(StringIO(response.text))

#========================================================================================================================================================== Hyper parametres

# ListSheetsName = 'Asm'
# #============== DataBase Section ====================
# rangeClear = "A2:ZZ"
# rangeUpdate = "A2"

# Asm = DataF(date_debut, date_fin, DC1, DC2, Asm["code_1"], Asm["code_2"], Asm["code_3"])
# ToGoogleSheets(Asm, SAMPLE_SPREADSHEET_ID, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])

SAMPLE_SPREADSHEET_ID_ASM = '1XCW5jyaGBdM111vFHk6wglkWi7VY27g6MJ_vNgpMXPg'
Asm = FromGoogleSheets(SAMPLE_SPREADSHEET_ID_ASM, SHEET_NAME='ASMs!A1:B')
Asm = Asm.rename(columns={'Superviseur': 'Asm Name', 'Salesman ID': 'salesman_employee_id'})


Asm = Asm[['salesman_employee_id', 'Asm Name']]
#=================================================================================================== Par Depot

# so_item = so_item.merge(df_listMaterial, on='Material Code', how='left')
so_item = so_item.merge(Asm, on='salesman_employee_id', how='left')
print(so_item)

ListSheetsName = 'SO'
#============== DataBase Section ====================
rangeClear = "A2:ZZ"
rangeUpdate = "A2"
import time
ToGoogleSheets(so_item, SAMPLE_SPREADSHEET_ID, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])
time.sleep(5)
Df_All = FromGoogleSheets(SAMPLE_SPREADSHEET_ID, SHEET_NAME='SO!A1:ZZ')

Df_All['Invoiced Qty Weight'] = Df_All['Invoiced Qty Weight'].astype(float)

Df_All['Item Invoice Amount'] = Df_All['Item Invoice Amount'].astype(float)

df_yola = Df_All.groupby(['Plant']).agg({'Customer ID' : 'nunique', 'Order No' : 'nunique', 'Invoiced Qty Weight' : 'sum', 'Item Invoice Amount' : 'sum'}).reset_index()

SAMPLE_SPREADSHEET_ID_Level = '1_hJLzedHrKPo9jSzhSfmA7uDzuc4v9BPWyKZ2J7RF4M'

ListSheetsName = 'Auto -- Yola Fresh & Plant Level'
#============== DataBase Section ====================
rangeClear = "A3:E"
rangeUpdate = "A3"
print(df_yola)
ToGoogleSheets(df_yola, SAMPLE_SPREADSHEET_ID_Level, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])

df_asm = Df_All.groupby(['Asm Name']).agg({'Customer ID' : 'nunique', 'Order No' : 'nunique', 'Invoiced Qty Weight' : 'sum', 'Item Invoice Amount' : 'sum'}).reset_index()

ListSheetsName = 'Auto -- ASM Level'
#============== DataBase Section ====================
rangeClear = "A3:E"
rangeUpdate = "A3"

ToGoogleSheets(df_asm, SAMPLE_SPREADSHEET_ID_Level, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])

# df_ambassador_Customers = Df_All[Df_All['Source'] == 'Salesman Order'].groupby(['salesman_employee_id']).agg({'Customer ID' : 'nunique'}).reset_index()
# df_ambassador_Customers = df_ambassador_Customers.rename(columns={'Customer ID': 'Unique Customers'})

df_ambassador = Df_All[Df_All['Source'] == 'Salesman Order'].groupby(['salesman_employee_id', 'Salesman Name', 'Invoice Date']).agg({'Customer ID' : 'nunique', 'Order No' : 'nunique', 'Invoiced Qty Weight' : 'sum', 'Item Invoice Amount' : 'sum'}).reset_index()

df_ambassador = df_ambassador.groupby(['salesman_employee_id', 'Salesman Name']).agg({'Customer ID' : 'sum', 'Order No' : 'sum', 'Invoiced Qty Weight' : 'sum', 'Item Invoice Amount' : 'sum'}).reset_index()

# df_ambassador = df_ambassador.merge(df_ambassador_Customers, on='salesman_employee_id', how='left')

df_ambassador = df_ambassador[['salesman_employee_id', 'Salesman Name', 'Customer ID', 'Order No', 'Invoiced Qty Weight', 'Item Invoice Amount']]

ListSheetsName = 'Auto -- Ambassador Level'
#============== DataBase Section ====================
rangeClear = "B3:G"
rangeUpdate = "B3"

ToGoogleSheets(df_ambassador, SAMPLE_SPREADSHEET_ID_Level, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])

df_subCategory = Df_All.groupby(['SubCatName']).agg({'Customer ID' : 'nunique', 'Order No' : 'nunique', 'Invoiced Qty Weight' : 'sum', 'Item Invoice Amount' : 'sum'}).reset_index()

ListSheetsName = 'Auto -- Sub Category Level'
#============== DataBase Section ====================
rangeClear = "A3:E"
rangeUpdate = "A3"

ToGoogleSheets(df_subCategory, SAMPLE_SPREADSHEET_ID_Level, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])

#========================================================================================================================================================================== W-1

# Week-1
date_debut_w1 = ((datetime.now()+timedelta(hours=0)) - timedelta(days=14)).strftime("%Y-%m-%d")
date_fin_w1 = ((datetime.now()+timedelta(hours=0)) - timedelta(days=9)).strftime("%Y-%m-%d")

so_item_w1 = DFSansParam(so_item_wow["code_1"], so_item_wow["code_2"], so_item_wow["code_3"], date_debut_w1, date_fin_w1, DC)

ListSheetsName = 'SO_W1'
#============== DataBase Section ====================
rangeClear = "A2:AI"
rangeUpdate = "A2"

ToGoogleSheets(so_item_w1, SAMPLE_SPREADSHEET_ID, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])
so_item_w1 = FromGoogleSheets(SAMPLE_SPREADSHEET_ID, SHEET_NAME='SO_W1!A1:AI')

so_item_w1['Invoice Date'] = so_item_w1['Invoice Date'].str.slice(0, 10)

so_item_w1['Invoice Date'] = pd.to_datetime(so_item_w1['Invoice Date'])
so_item_w1['Invoice Date'] = so_item_w1['Invoice Date'].dt.strftime("%d-%m-%Y")

# so_item_w1 = so_item_w1.merge(df_listMaterial, on='Material Code', how='left')
so_item_w1 = so_item_w1.merge(Asm, on='salesman_employee_id', how='left')


ListSheetsName = 'SO_W1'
#============== DataBase Section ====================
rangeClear = "A2:ZZ"
rangeUpdate = "A2"

ToGoogleSheets(so_item_w1, SAMPLE_SPREADSHEET_ID, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])

Df_All_w1 = FromGoogleSheets(SAMPLE_SPREADSHEET_ID, SHEET_NAME='SO_W1!A1:ZZ')

Df_All_w1['Invoiced Qty Weight'] = Df_All_w1['Invoiced Qty Weight'].astype(float)
Df_All_w1['Item Invoice Amount'] = Df_All_w1['Item Invoice Amount'].astype(float)

df_yola_w1 = Df_All_w1.groupby(['Plant']).agg({'Customer ID' : 'nunique', 'Order No' : 'nunique', 'Invoiced Qty Weight' : 'sum', 'Item Invoice Amount' : 'sum'}).reset_index()
df_yola_w1 = df_yola_w1.rename(columns={'Customer ID': 'Customer ID W1', 'Order No' : 'Order No W1', 'Invoiced Qty Weight' : 'Invoiced Qty Weight W1', 'Item Invoice Amount' : 'Item Invoice Amount W1'})

df_yola_w1 = df_yola[['Plant']].merge(df_yola_w1, on='Plant', how='left')

df_yola_w1 = df_yola_w1[['Customer ID W1', 'Order No W1', 'Invoiced Qty Weight W1', 'Item Invoice Amount W1']]

ListSheetsName = 'Auto -- Yola Fresh & Plant Level'
#============== DataBase Section ====================
rangeClear = "I3:L"
rangeUpdate = "I3"

ToGoogleSheets(df_yola_w1, SAMPLE_SPREADSHEET_ID_Level, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])

df_asm_w1 = Df_All_w1.groupby(['Asm Name']).agg({'Customer ID' : 'nunique', 'Order No' : 'nunique', 'Invoiced Qty Weight' : 'sum', 'Item Invoice Amount' : 'sum'}).reset_index()

df_asm_w1 = df_asm[['Asm Name']].merge(df_asm_w1, on='Asm Name', how='left')

df_asm_w1 = df_asm_w1[['Customer ID', 'Order No', 'Invoiced Qty Weight', 'Item Invoice Amount']]

ListSheetsName = 'Auto -- ASM Level'
#============== DataBase Section ====================
rangeClear = "I3:L"
rangeUpdate = "I3"

ToGoogleSheets(df_asm_w1, SAMPLE_SPREADSHEET_ID_Level, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])

# df_ambassador_Customers_w1 = Df_All_w1[Df_All_w1['Source'] == 'Salesman Order'].groupby(['salesman_employee_id']).agg({'Customer ID' : 'nunique'}).reset_index()
# df_ambassador_Customers_w1 = df_ambassador_Customers_w1.rename(columns={'Customer ID': 'Unique Customers'})

df_ambassador_w1 = Df_All_w1[Df_All_w1['Source'] == 'Salesman Order'].groupby(['salesman_employee_id', 'Invoice Date']).agg({'Customer ID' : 'nunique', 'Order No' : 'nunique', 'Invoiced Qty Weight' : 'sum', 'Item Invoice Amount' : 'sum'}).reset_index()

df_ambassador_w1 = df_ambassador_w1.groupby(['salesman_employee_id']).agg({'Customer ID' : 'sum', 'Order No' : 'sum', 'Invoiced Qty Weight' : 'sum', 'Item Invoice Amount' : 'sum'}).reset_index()

# df_ambassador_w1 = df_ambassador_w1.merge(df_ambassador_Customers_w1, on='salesman_employee_id', how='left')

df_ambassador_w1 = df_ambassador[['salesman_employee_id']].merge(df_ambassador_w1, on='salesman_employee_id', how='left')

df_ambassador_w1 = df_ambassador_w1[['Customer ID', 'Order No', 'Invoiced Qty Weight', 'Item Invoice Amount']]

ListSheetsName = 'Auto -- Ambassador Level'
#============== DataBase Section ====================
rangeClear = "K3:N"
rangeUpdate = "K3"

ToGoogleSheets(df_ambassador_w1, SAMPLE_SPREADSHEET_ID_Level, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])

df_subCategory_w1 = Df_All_w1.groupby(['SubCatName']).agg({'Customer ID' : 'nunique', 'Order No' : 'nunique', 'Invoiced Qty Weight' : 'sum', 'Item Invoice Amount' : 'sum'}).reset_index()

df_subCategory_w1 = df_subCategory[['SubCatName']].merge(df_subCategory_w1, on='SubCatName', how='left')

df_subCategory_w1 = df_subCategory_w1[['Customer ID', 'Order No', 'Invoiced Qty Weight', 'Item Invoice Amount']]

ListSheetsName = 'Auto -- Sub Category Level'
#============== DataBase Section ====================
rangeClear = "I3:L"
rangeUpdate = "I3"

ToGoogleSheets(df_subCategory_w1, SAMPLE_SPREADSHEET_ID_Level, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets'])



