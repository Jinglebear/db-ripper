import time
import requests
import csv


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
stations = readStation("..\\misc\\Mappe1.csv")


def getEvaNum(station, base_request_string, header, resultArr, failArr):
    response = requests.get(
        base_request_string+station[1], headers=header)
    response_status_code = response.status_code
    response = response.json()  # json encoding of response
    if(response_status_code == 200):  # sucessful response code
        evaNumber = response['result'][0]['evaNumbers'][0]['number']
        eva_triplet = (station[0], evaNumber, station[2])
        resultArr.append(eva_triplet)
        print(eva_triplet, flush=True)  # debug
    else:
        print("ERROR: "+str(response_status_code), flush=True)  # debug
        failArr.append(station)


def computeEvaNums(stations, base_request_string, headers, resultArr, failArr):
    counter1 = 0
    counter2 = 0
    counter3 = 0
    counter4 = 0
    for station in stations:
        try:
            if (counter1 < 100):
                counter1 += 1
                getEvaNum(station=station, base_request_string=base_request_string,
                          header=headers[0], resultArr=resultArr, failArr=failArr)
            elif(counter2 < 100):
                counter2 += 1
                getEvaNum(station=station, base_request_string=base_request_string,
                          header=headers[1], resultArr=resultArr, failArr=failArr)
            elif(counter3 < 100):
                counter3 += 1
                getEvaNum(station=station, base_request_string=base_request_string,
                          header=headers[2], resultArr=resultArr, failArr=failArr)
            elif(counter4 < 100):
                counter4 += 1
                getEvaNum(station=station, base_request_string=base_request_string,
                          header=headers[3], resultArr=resultArr, failArr=failArr)
            if(counter1 == 100 and counter2 == 100 and counter3 == 100 and counter4 == 100):
                time.sleep(60)
                counter1 = 0
                counter2 = 0
                counter3 = 0
                counter4 = 0
        except IndexError:
            print("ERROR: IndexError", flush=True)  # debug
            failArr.append(station)
            failArr.append("(IndexError)")
        except:
            print("ERROR: UnkownError", flush=True)  # debug


base_request_string = "https://api.deutschebahn.com/stada/v2/stations/"
resultArr = []
failArr = []
tokenArr = ['Bearer c63151b12eb8b7a48ef869a72c77c15f', 'Bearer 0548da9b09541d93c01c2685f5e8a611',
            'Bearer 9813ceeae64e273dca14d615405395d7', 'Bearer 194291fed31c8daf95c8d45f22254399']
headers_arr = [{'Accept': 'application/json',
               'Authorization': tokenArr[0]}, {'Accept': 'application/json',
               'Authorization': tokenArr[1]}, {'Accept': 'application/json',
               'Authorization': tokenArr[2]}, {'Accept': 'application/json',
               'Authorization': tokenArr[3]}]
computeEvaNums(stations, base_request_string, headers_arr, resultArr, failArr)
with open("..\\misc\\table-1-result.csv", "w", encoding='utf-8') as resultfile:
    writer = csv.writer(resultfile)
    for result in resultArr:
        writer.writerow(result)
with open("..\\misc\\table-1-fail.csv", "w", encoding='utf-8') as failfile:
    writer = csv.writer(failfile)
    for fail in failArr:
        writer.writerow(fail)
