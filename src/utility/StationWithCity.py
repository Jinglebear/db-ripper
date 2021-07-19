import time
import requests
import csv
import sys
from itertools import groupby

def all_equal(counterArr):
    g = groupby(counterArr)
    return next(g,True) and not next(g,False)


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


# array with the important station data
stations = readStation("/home/nils/db-ripper/misc/Mappe1.csv")


def getEvaNum(station, base_request_string, header, resultArr, failArr):
    response = requests.get(
        base_request_string+station[1], headers=header)
    response_status_code = response.status_code
    response = response.json()  # json encoding of response
    if(response_status_code == 200):  # sucessful response code
        evaNumber = response['result'][0]['evaNumbers'][0]['number']
        city = response['result'][0]['mailingAddress']['city']
        eva_quadrupel = (station[0], evaNumber, station[2],city)
        resultArr.append(eva_quadrupel)
        #print(eva_triplet, flush=True)  # debug
    else:
        print("ERROR: "+str(response_status_code), flush=True)  # debug
        failArr.append(station)


def computeEvaNums(stations, base_request_string, headers, resultArr, failArr, counterArr):
    control = 0
    for station in stations:
        
        control +=1
        print(control,flush=True)
        try:
            for i in range(len(counterArr)):
                if(counterArr[i] < 100):
                    counterArr[i] +=1
                    getEvaNum(station=station, base_request_string=base_request_string,
                    header=headers[i], resultArr=resultArr, failArr=failArr)
                    break
            sleep = all_equal(counterArr=counterArr)
            if(sleep):
                print("<<<<<<<<<<<<<<SLEEPING>>>>>>>>>>>>>>>>",flush=True)
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


base_request_string = "https://api.deutschebahn.com/stada/v2/stations/"
resultArr = []
failArr = []
tokenArr = ['Bearer c63151b12eb8b7a48ef869a72c77c15f', 'Bearer 0548da9b09541d93c01c2685f5e8a611',
            'Bearer 9813ceeae64e273dca14d615405395d7', 'Bearer 194291fed31c8daf95c8d45f22254399',
            'Bearer fca48279e7030608b9799ab4e84c8975', 'Bearer b4a1b8aa689f4b1f2666af53a1b592ab',
            'Bearer 8312dec31d1808720aaadd9334accb24', 'Bearer eed6fa376f86dce95beb400315616e58']
headers_arr =[]
for token in tokenArr:
    header = {'Accept':'application/json', 'Authorization' : token}
    headers_arr.append(header)
counterArr =[]
for i in range(8):
    counterArr.append(0)

computeEvaNums(stations, base_request_string, headers_arr, resultArr, failArr,counterArr=counterArr)
with open("/home/nils/db-ripper/misc/table-2-result.csv", "w", newline='', encoding='utf-8') as resultfile:
    writer = csv.writer(resultfile)
    for result in resultArr:
        writer.writerow(result)
with open("/home/nils/db-ripper/misc/table-2-fail.csv", "w", newline='', encoding='utf-8') as failfile:
    writer = csv.writer(failfile)
    for fail in failArr:
        writer.writerow(fail)