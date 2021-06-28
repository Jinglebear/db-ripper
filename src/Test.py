import requests 
import json
import pprint
 

headers = {
    'Accept': 'application/json',
    'Authorization': 'Bearer 873be58d3db312b4e52a2102e5641c27',
}

 

response = requests.get('https://api.deutschebahn.com/stada/v2/stations/82', headers=headers)
data = json.loads(response.text)
#print(type(data))
#result = data['result']
#print(type(result))
#evaNumbers = result[0]['evaNumbers']
#print(type(evaNumbers))
pp = pprint.PrettyPrinter()


result_list = data['result']
#print(type(result_list))
#pp.pprint(result_list)

evaNumbers_list = result_list[0]['evaNumbers']
#print(type(evaNumbers_list))
evaNumber = evaNumbers_list[0]['number']
print(evaNumber)
