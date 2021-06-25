import requests
import xml.etree.ElementTree as ET
import time
import csv
# read in city names from csv
def readCity(filename):
    cities = []
    with open(filename, "r") as file:
        for line in file:
            city = line.strip().split(';')
            city_name = city[1]
            cities.append(city_name)
    return cities
def computeEvaNums(cities, base_request_string, authToken,resultArr):
    headers = {
        'Accept': 'application/xml',
        'Authorization': authToken
    }
    counter = 0
    for city in cities:
        if (counter < 18):
            #time.sleep(60)
            #counter = 0 # COMMENT THIS LINE OUT FOR TESTING
            response = requests.get(base_request_string+city, headers=headers)
            counter = counter + 1
            response_status_code = response.status_code
            if(response_status_code == 200):
                tree = ET.fromstring(response.text)
                for station in tree.findall('station'):
                    eva = station.get('eva')
                    # print(station.attrib)
                    name = station.get('name')
                    tupel = (name,eva)
                    resultArr.append(tupel)
                    print(name + ' ' + eva)
            else:
                print("fail")

# array with all city names
cities = readCity("..\\misc\\table-1.csv")



# the base request string for recieving eva numbers
base_request_string = "https://api.deutschebahn.com/timetables/v1/station/"
resultArr = []
computeEvaNums(cities,base_request_string,'Bearer 873be58d3db312b4e52a2102e5641c27',resultArr)

#time.sleep(120)
with open("..\\misc\\table-1-result.csv","w") as resultfile:
    writer = csv.writer(resultfile)
    for result in resultArr:
        writer.writerow(result)





    
