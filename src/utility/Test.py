import requests

header = {'Accept':'application/json', 'Authorization' : 'Bearer b4a1b8aa689f4b1f2666af53a1b592ab'}

base_request_string = "https://api.deutschebahn.com/stada/v2/stations/2"

response = requests.get(base_request_string,headers=header)
#print(response.status_code)

response = response.json()
#print(response)
evaNumber = response['result'][0]['evaNumbers'][0]['number']

city = response['result'][0]['mailingAddress']['city']

print(city)

