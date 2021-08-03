import requests
import time
import csv
import sys
from itertools import groupby
import Utils


def readStation(filename):
    stations = []
    with open(filename, encoding='utf-8') as file:
        skip = True  # skip first row of csv file
        reader = csv.reader(file)
        for row in reader:
            if(not skip):
                splitted_string = row[0].split(',')
                station_triplet = (
                    splitted_string[1], splitted_string[0], splitted_string[4])
                stations.append(station_triplet)
            skip = False
    return stations


def getGeoData(station, base_request_string, header, resultArr, failArr):
    with requests.Session() as session:
        response = session.get(
            base_request_string+station[1], headers=header)
        response_status_code = response.status_code
        response = response.json()  # json encoding of response
        if(response_status_code == 200):  # sucessful response code
            city = response['result'][0]['mailingAddress']['city']
            geographicCoordinates = response['result'][0]['evaNumbers'][0]['geographicCoordinates']
            station_enriched = (station[0], station[2],
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


def send_on_success(record_metadata):
    print('topic:', record_metadata.topic,
          'partition:', record_metadata.partition)


# array with the important station data
stations = readStation("/home/nils/db-ripper/misc/Mappe1.csv")


base_request_string = "https://api.deutschebahn.com/stada/v2/stations/"
resultArr = []
failArr = []

tokenArr = Utils.tokenlistTimePark
headers_arr = []
for token in tokenArr:
    header = {'Accept': 'application/json', 'Authorization': token}
    headers_arr.append(header)
counterArr = []
for i in range(30):
    counterArr.append(0)

computeGeoData(stations, base_request_string, headers_arr,
               resultArr, failArr, counterArr=counterArr)

with open("/home/nils/db-ripper/misc/test_table_result.csv", "w", newline='', encoding='utf-8') as resultfile:
    writer = csv.writer(resultfile)
    for result in resultArr:
        writer.writerow(result)
with open("/home/nils/db-ripper/misc/test_table_fail.csv", "w", newline='', encoding='utf-8') as failfile:
    writer = csv.writer(failfile)
    for fail in failArr:
        writer.writerow(fail)
