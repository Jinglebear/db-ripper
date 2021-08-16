import csv
import requests
from itertools import groupby
import time
import sys
import Utils
def readStation(filename):
    stations = []
    with open(filename, encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            try:
                #station name == first Element
                #station id == second Element
                #station category == third Element
                station = (row[0],row[1])
                category = int(row[2])
                if(category <=4):
                    stations.append(station)
            except Exception as e:
                print(e)
                print(row)
    return stations

def getGeoData(station, base_request_string, header, resultArr, failArr):
    with requests.Session() as session:
        response = session.get(
            base_request_string+station[1], headers=header)
        response_status_code = response.status_code
        response = response.json()  # json encoding of response
        if(response_status_code == 200):  # sucessful response code
            #get the city Name from API
            city = response['result'][0]['mailingAddress']['city']
            #get location (long,lat) from API
            geographicCoordinates = response['result'][0]['evaNumbers'][0]['geographicCoordinates']
            #get category from API
            category = response['result'][0]['category']
            station_enriched = (station[0], category,
                                city, geographicCoordinates)
            resultArr.append(station_enriched)
            # print(eva_triplet, flush=True)  # debug
        else:
            print("ERROR: "+str(response_status_code), flush=True)  # debug
            failArr.append(station)


def all_equal(counterArr):
    g = groupby(counterArr)
    return next(g, True) and not next(g, False)

def computeGeoData(stations, base_request_string, headers, resultArr, failArr, counterArr):
    control = 0
    for station in stations:

        control += 1
        print(control, flush=True)
        try:
            for i in range(len(counterArr)):
                if(counterArr[i] < 100):
                    counterArr[i] += 1
                    getGeoData(station=station, base_request_string=base_request_string,
                               header=headers[i], resultArr=resultArr, failArr=failArr)
                    break
            sleep = all_equal(counterArr=counterArr)
            if(sleep):
                print("<<<<<<<<<<<<<<SLEEPING>>>>>>>>>>>>>>>>", flush=True)
                time.sleep(61)
                for j in range(len(counterArr)):
                    counterArr[j] = 0
        except IndexError:
            print("ERROR: IndexError", flush=True)  # debug
            failArr.append(station)
            failArr.append("(IndexError)")
        except:
            e = sys.exc_info()
            print(e)



stations = readStation("..\\..\\misc\\test_table_fail.csv")

base_request_string = "https://api.deutschebahn.com/stada/v2/stations/"
resultArr = []
failArr = []

tokenArr = Utils.tokens_timetable_parking
headers_arr = []
for token in tokenArr:
    header = {'Accept': 'application/json', 'Authorization': token}
    headers_arr.append(header)
counterArr = []
for i in range(30):
    counterArr.append(0)
computeGeoData(stations, base_request_string, headers_arr,
               resultArr, failArr, counterArr=counterArr)


with open("..\\..\\misc\\table_stations_location_sorted.csv","a",newline='',encoding='utf-8')as outputfile:
    writer = csv.writer(outputfile)
    for result in resultArr:
        writer.writerow(result)

with open("..\\..\\misc\\fail.csv","w",newline='',encoding='utf-8') as failfile:
    writer = csv.writer(failfile)
    for fail in failArr:
        writer.writerow(fail)