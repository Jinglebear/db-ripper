import requests
import xml.etree.ElementTree as ET


# read in city names from csv
def readCity(filename):
    cities = []
    with open (filename,"r") as file:
        for line in file:
            city = line.strip().split(';')
            city_name = city[1]
            cities.append(city_name)
    return cities


# array with all city names
cities = readCity("..\\misc\\table-1.csv")
print(cities)


# the base request string for recieving eva numbers
base_request_string = "https://api.deutschebahn.com/timetables/v1/station/"

# the header containing the auth token for api
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





