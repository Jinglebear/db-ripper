import requests
import xml.etree.ElementTree as ET
base_request_string = "https://api.deutschebahn.com/timetables/v1/station/"
headers = {
    'Accept': 'application/xml',
    'Authorization': 'Bearer 873be58d3db312b4e52a2102e5641c27'
}
any_station_name = 'Mainz'
response = requests.get(base_request_string+any_station_name,headers=headers)
tree = ET.fromstring(response.text)
for station in tree.findall('station'):
    eva = station.get('eva')
    #print(station.attrib)
    print(station.get('name'),eva)