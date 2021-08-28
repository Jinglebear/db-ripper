import time
import requests
import csv
import sys
from itertools import groupby

# ====================================================================
# **Description**
# This scripts does exactly the same as "Station.py"
# But : also collects the city name belonging to the train station


def all_equal(counter_arr):
    g = groupby(counter_arr)
    return next(g,True) and not next(g,False)


def read_station(filename):
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
stations = read_station("..\\..\\misc\\Mappe1.csv")


def get_eva_num(station, base_request_string, header, result_arr, fail_arr):
    response = requests.get(
        base_request_string+station[1], headers=header)
    response_status_code = response.status_code
    response = response.json()  # json encoding of response
    if(response_status_code == 200):  # sucessful response code
        eva_number = response['result'][0]['evaNumbers'][0]['number']
        city = response['result'][0]['mailingAddress']['city']
        eva_quadrupel = (station[0], eva_number, station[2],city)
        result_arr.append(eva_quadrupel)
        #print(eva_triplet, flush=True)  # debug
    else:
        print("ERROR: "+str(response_status_code), flush=True)  # debug
        fail_arr.append(station)


def compute_eva_nums(stations, base_request_string, headers, result_arr, fail_arr, counter_arr):
    control = 0
    for station in stations:
        
        control +=1
        print(control,flush=True)
        try:
            for i in range(len(counter_arr)):
                if(counter_arr[i] < 100):
                    counter_arr[i] +=1
                    get_eva_num(station=station, base_request_string=base_request_string,
                    header=headers[i], result_arr=result_arr, fail_arr=fail_arr)
                    break
            sleep = all_equal(counter_arr=counter_arr)
            if(sleep):
                print("<<<<<<<<<<<<<<SLEEPING>>>>>>>>>>>>>>>>",flush=True)
                time.sleep(61)
                for j in range(len(counter_arr)):
                    counter_arr[j] = 0
        except IndexError:
            print("ERROR: IndexError", flush=True)  # debug
            fail_arr.append(station)
            fail_arr.append("(IndexError)")
        except:
            e = sys.exc_info()
            print(e)


base_request_string = "https://api.deutschebahn.com/stada/v2/stations/"
result_arr = []
fail_arr = []
tokenArr = ['Bearer c63151b12eb8b7a48ef869a72c77c15f', 'Bearer 0548da9b09541d93c01c2685f5e8a611',
            'Bearer 9813ceeae64e273dca14d615405395d7', 'Bearer 194291fed31c8daf95c8d45f22254399',
            'Bearer fca48279e7030608b9799ab4e84c8975', 'Bearer b4a1b8aa689f4b1f2666af53a1b592ab',
            'Bearer 8312dec31d1808720aaadd9334accb24', 'Bearer eed6fa376f86dce95beb400315616e58']
headers_arr =[]
for token in tokenArr:
    header = {'Accept':'application/json', 'Authorization' : token}
    headers_arr.append(header)
counter_arr =[]
for i in range(8):
    counter_arr.append(0)

compute_eva_nums(stations, base_request_string, headers_arr, result_arr, fail_arr,counter_arr=counter_arr)
with open("..\\..\\misc\\table-2-result.csv", "w", newline='', encoding='utf-8') as resultfile:
    writer = csv.writer(resultfile)
    for result in result_arr:
        writer.writerow(result)
with open("..\\..\\misc\\table-2-fail.csv", "w", newline='', encoding='utf-8') as failfile:
    writer = csv.writer(failfile)
    for fail in fail_arr:
        writer.writerow(fail)