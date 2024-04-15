import requests
from yola_functions import getToken

token = getToken("ayyoub.mahraz@yolafresh.com","U2FsdGVkX1/LKJPzIgYNIgoRSFWDp1KDBFuDMKnqqEs=","DMS","admin")

url = "https://admin-compass-api.censanext.com/api/v1/priceLevel/priceLevelArchiveCsv?from=2024-03-26&days=15&plant=2000001"

payload = {}
headers = {
  'authority': 'admin-compass-api.censanext.com',
  'accept': '*/*',
  'accept-language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
  'access-control-request-headers': 'x-access-token',
  'access-control-request-method': 'GET',
  'origin': 'https://admin-compass.censanext.com',
  'referer': 'https://admin-compass.censanext.com/',
  'sec-fetch-dest': 'empty',
  'sec-fetch-mode': 'cors',
  'sec-fetch-site': 'same-site',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'x-access-token': token
}


response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
