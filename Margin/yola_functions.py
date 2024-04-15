#Good, Done
import pandas as pd
import json
import numpy as np
import requests
from io import StringIO
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from google.oauth2 import service_account
from json import dumps
from httplib2 import Http
import gspread
from oauth2client.service_account import ServiceAccountCredentials

#Commentaire

def sOItemMetabase(JourDebut, JourFin, DC) :

    date_debut = ((datetime.now()+timedelta(hours=0)) - timedelta(days=JourDebut)).strftime("%Y-%m-%d")
    date_fin = ((datetime.now()+timedelta(hours=0)) - timedelta(days=JourFin)).strftime("%Y-%m-%d")

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

    headers = {
    "Cookie": cookies,
    "Cache-Control": "no-cache",
    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
    }

    url = f"https://insights.censanext.com/api/dashboard/518/dashcard/5155/card/4296/query/csv"

    if  DC == None :
        data = {
    "parameters":f'[{{"type":"string/=","value":["2000001", "2000002"],"id":"b2e6005a","target":["dimension",["field","plant",{{"base-type":"type/Text"}}]]}},{{"type":"date/all-options","value":"{str(date_debut)}~{str(date_fin)}","id":"60ef3c44","target":["dimension",["field","order_date",{{"base-type":"type/DateTime"}}]]}},{{"type":"date/all-options","id":"99f2e497","target":["dimension",["field","invoice_date",{{"base-type":"type/DateTime"}}]]}}]'
    }
    else :

        data = {
    "parameters":f'[{{"type":"string/=","value":["{DC}"],"id":"b2e6005a","target":["dimension",["field","plant",{{"base-type":"type/Text"}}]]}},{{"type":"date/all-options","value":"{str(date_debut)}~{str(date_fin)}","id":"60ef3c44","target":["dimension",["field","order_date",{{"base-type":"type/DateTime"}}]]}},{{"type":"date/all-options","id":"99f2e497","target":["dimension",["field","invoice_date",{{"base-type":"type/DateTime"}}]]}}]'
        }

    # Effectuez la requête HTTP
    response = requests.post(url, data=data, headers=headers)

    # Assurez-vous que le texte est correctement encodé (en supposant que le texte est en UTF-8)
    response.encoding = 'utf-8'
    so_item = pd.read_csv(StringIO(response.text))
    so_item['Order Date'] = so_item['order_date'].str.slice(0, 10)
    so_item['Order time'] = so_item['order_date'].str.slice(11, 19)
    so_item['Item Amount'] = so_item['total_order_amount']
    so_item['Item Invoice Price'] = so_item['invoice_amount']
    so_item['Item Invoice Amount'] = so_item['invoice_amount']

    so_item['Order Date'] = pd.to_datetime(so_item['Order Date'])
    so_item['Order Date'] = so_item['Order Date'].dt.strftime("%d-%m-%Y")

    so_item['delivery_date'] = pd.to_datetime(so_item['delivery_date'])
    so_item['delivery_date'] = so_item['delivery_date'].dt.strftime("%d-%m-%Y")
    so_item['invoice_number'] = so_item['invoice_number'].astype(str)
    so_item['invoice_number'] = so_item['invoice_number'].replace("nan", "--")

    if DC != None :
        so_item = so_item[so_item['plant'] == DC]
    liste_columns = ['order_number', 'Order Date', 'Order time', 'delivery_date', 'customer_id', 'customer_name', 'material_code', 'material_name', 'category', 'sub_category', 'moq', 'order_set', 'total_order_quantity', 'item_price', 'Item Amount', 'total_order_amount', 'invoice_number', 'invoice_quantity', 'discount_amount', 'Item Invoice Price', 'Item Invoice Amount', 'invoice_amount', 'partial_invoice_remark', 'uom', 'customer_mobile', 'customer_pincode', 'customer_region', 'customer_zone', 'customer_latitude', 'customer_longitude', 'so_source', 'salesman_name', 'salesman_id', 'order_status']

    return so_item[liste_columns]

SERVICE_ACCOUNT_FILE= 'keys.json'
SCOPES=['https://www.googleapis.com/auth/spreadsheets']


def fromGoogleSheets(SAMPLE_SPREADSHEET_ID, SHEET_NAME, SCOPES=['https://www.googleapis.com/auth/spreadsheets']):
        try :
                creds = None
                creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)

                service = build('sheets', 'v4', credentials=creds)

                # Call the Sheets API
                sheet = service.spreadsheets()

                # Appeler l'API Sheets pour récupérer toutes les données de la feuille de calcul
                result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SHEET_NAME).execute()

                # Extraire les valeurs de la réponse
                values = result.get('values', [])

                # Vérifier si des données ont été récupérées
                if not values:
                    print('Aucune donnée trouvée.')
                else:
                    # Créer un DataFrame pandas à partir des données
                    df = pd.DataFrame(values[1:], columns=values[0])

                    # Afficher le DataFrame
                    # print(df)

                    print("resultat")

        except Exception as e :
            return print("Erreur : ", str(e))

        return df

def toGoogleSheets(DataBase, SAMPLE_SPREADSHEET_ID, ListSheetsName, rangeClear, rangeUpdate, SCOPES=['https://www.googleapis.com/auth/spreadsheets']):
    try :
            creds = None
            creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)

            service = build('sheets', 'v4', credentials=creds)

            # Call the Sheets API
            sheet = service.spreadsheets()

            #============== List of Sheet's Names
            ListSheetsNames = [ListSheetsName]

            #=============== CalculSalle
            for SheetName in ListSheetsNames:

                    request = service.spreadsheets().values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SheetName+"!"+rangeClear).execute()
                    Data = DataBase

                    Data.fillna('', inplace=True)

                    Data = castForGsheets(Data)

                    Data = Data.values.tolist()

                    request = service.spreadsheets().values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SheetName+"!"+rangeUpdate, valueInputOption="USER_ENTERED", body={"values":Data}).execute()
    except Exception as e :
            return print('Erreur : ', str(e))

def castForGsheets(df):
    # casting as string if not serializable
    for column, dt in zip(df.columns, df.dtypes):
        if dt.type not in [
            np.int64,
            np.float_,
            np.bool_,
        ]:
            df.loc[:, column] = df[column].astype(str)
    return df

def dashboardDatamart(code_1, code_2, code_3, params) :

    url = "http://4.178.96.144:3000/api/session"

    payload = json.dumps({
    "username": "asaad.ounzar@yolafresh.com",
    "password": "Asaad123@@",
    "remember": True
    })
    headers = {
    'Content-Type': 'application/json',
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.headers.get('Set-Cookie'))
    cookies = response.headers.get('Set-Cookie')

    headers = {
    "Cookie": cookies,
    "Cache-Control": "no-cache",
    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
    }

    url = f"http://4.178.96.144:3000/api/dashboard/{code_1}/dashcard/{code_2}/card/{code_3}/query/csv"

    data = {
    "parameters":params
    }

    response = requests.post(url, data=data, headers=headers)
    response.encoding = 'utf-8'

    return pd.read_csv(StringIO(response.text))


def NotifGChat(url_input, body) :

    today = datetime.now()-timedelta(days=0)
    today = today + timedelta(hours=0)
    today = today.strftime("%d/%m/%Y %H:%M")

    url = url_input
    bot_message = {
        'text' : f''' Date : {today}

{body}
'''}

    message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
    http_obj = Http()

    http_obj.request(
        uri=url,
        method='POST',
        headers=message_headers,
        body=dumps(bot_message),
    )

def printf():
     print('zz')
def getToken(login, password, token_type, portal=None) :
    
    if token_type == 'DMS' :
        # URL de l'API
        url = "https://userlogin-compass-api.censanext.com/v1/admin/login"

        # Données du corps de la demande
        data = {
            "email": login,
            "password": password,

            "companyCode": "20000",
            "portalType": portal
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

        else:
            # Erreur

            error_message = response_data.get("message")
            print("Erreur lors de la connexion:", error_message)

    elif token_type == 'Metabase' :
        url = "https://insights.censanext.com/api/session"
        payload = json.dumps({
            "username": login,
            "password": password,
            "remember": True
        })
        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.headers.get('Set-Cookie'))
        access_token = response.headers.get('Set-Cookie')
    return access_token

def get_dataframe_metabase(parameters,code_1, code_2, code_3, token) : 

    headers = {
    "Cookie" : token,
    "Cache-Control": "no-cache",
    'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
    }

    url = f"https://insights.censanext.com/api/dashboard/{code_1}/dashcard/{code_2}/card/{code_3}/query/csv"

    data = {
    # "parameters": f'[{{"type":"string/=","value":["{DC1}","{DC2}"],"id":"838104f8","target":["dimension",["field",23484,null]]}}]
    "parameters": parameters
     }

    # Effectuez la requête HTTP
    response = requests.post(url, data=data, headers=headers)

    # Assurez-vous que le texte est correctement encodé (en supposant que le texte est en UTF-8)
    response.encoding = 'utf-8'

    return pd.read_csv(StringIO(response.text))


def addBlanckRow(SAMPLE_SPREADSHEET_ID,sheet_name):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('keys.json', scope)
        client = gspread.authorize(creds)

        spreadsheet = client.open_by_key(SAMPLE_SPREADSHEET_ID)

        worksheet = spreadsheet.worksheet(sheet_name)

        longeur = len(fromGoogleSheets(SAMPLE_SPREADSHEET_ID, SHEET_NAME=sheet_name+'!A1:Z'))

        worksheet.add_rows(1)