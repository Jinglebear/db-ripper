import requests 
import xml.etree.ElementTree as ET 

 

headers = {
    'Accept': 'application/xml',
    'Authorization': 'Bearer 13d747ec4d3615f93cca7dcf7f203389',
}

 

response = requests.get('https://api.deutschebahn.com/timetables/v1/rchg/8000240', headers=headers)
tree = ET.fromstring(response.text)
for element in tree:
    print(element)
    print(element.tag)
    print(element.attrib)
    print()