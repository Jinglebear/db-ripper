from os import error
import requests
import xml.etree.ElementTree as ET
import time
import csv
import json
### readCityIDs and Names
# 
# read in city IDS and names from file
def readCityIDs(filename):
    cityIDS = []
    with open(filename, "r") as file:
        for line in file:
            city = line.strip().split(';')
            city_ID = city[0]
            city_name = city[1]
            city_tupel = (city_name,city_ID)
            cityIDS.append(city_tupel)
    return cityIDS


# array with all city ID's 
cityIDs = readCityIDs("..\\misc\\table-1.csv")


### computeEvaNums
#
# @param city(array containing city names that own railway stations)
# @param base_request_string (the URL used for this request)
# @param authToken (the authorization Token used for the DB API)
# @param resultArr (the array were all railway station names with belonging eva numbers are stored)
# 
# collect every eva number for the station in cities (array)
# write the resulting table into a .csv
def computeEvaNums(cityIDs, base_request_string, authToken,resultArr,failArr):
    headers = {
        'Accept': 'application/json',
        'Authorization': authToken
    }
    counter = 0
    for city_ID in cityIDs:
        if (counter < 100):
            response = requests.get(base_request_string+city_ID[1], headers=headers)
            counter = counter + 1
            response_status_code = response.status_code
            if(response_status_code == 200):
                data = json.loads(response.text)
                try:
                    result_list = data['result']
                    evaNumbers_list = result_list[0]['evaNumbers']
                    evaNumber = evaNumbers_list[0]['number']
                    eva_tupel = (city_ID[0],evaNumber)
                    resultArr.append(eva_tupel)
                    print(eva_tupel,flush=True)
                except IndexError:
                    print("Fehler! :-(")
                    failArr.append(city_ID)
                    failArr.append("(IndexError)")
                except:
                    print('Fehler ! :-(')
            else:
                print("fail")
                failArr.append(city_ID)
        else:
            print('sleeping...')
            time.sleep(60)
            counter = 0 




# the base request string for recieving eva numbers
base_request_string = "https://api.deutschebahn.com/stada/v2/stations/"
resultArr = []
failArr = []
computeEvaNums(cityIDs,base_request_string,'Bearer 873be58d3db312b4e52a2102e5641c27',resultArr,failArr)

#time.sleep(120)
with open("..\\misc\\table-1-result.csv","w") as resultfile:
    writer = csv.writer(resultfile)
    for result in resultArr:
        writer.writerow(result)
with open("..\\misc\\table-1-fail.csv","w") as failfile:
    writer = csv.writer(failfile)
    for fail in failArr:
        writer.writerow(fail)



    
