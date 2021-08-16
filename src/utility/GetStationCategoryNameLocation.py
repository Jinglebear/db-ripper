import requests
import time
import csv
import sys
from itertools import groupby
import Utils


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


def get_geo_data(station, base_request_string, header, result_arr, fail_arr):
    with requests.Session() as session:
        response = session.get(
            base_request_string+station[1], headers=header)
        response_status_code = response.status_code
        response = response.json()  # json encoding of response
        if(response_status_code == 200):  # sucessful response code
            city = response['result'][0]['mailingAddress']['city']
            geographic_coordinates = response['result'][0]['evaNumbers'][0]['geographicCoordinates']
            station_enriched = (station[0], station[2],
                                city, geographic_coordinates)
            result_arr.append(station_enriched)
            # print(eva_triplet, flush=True)  # debug
        else:
            print("ERROR: "+str(response_status_code), flush=True)  # debug
            fail_arr.append(station)


def all_equal(counterArr):
    g = groupby(counterArr)
    return next(g, True) and not next(g, False)


def compute_geo_data(stations, base_request_string, headers, result_arr, fail_arr, counter_arr):
    control = 0
    for station in stations:

        control += 1
        print(control, flush=True)
        try:
            for i in range(len(counter_arr)):
                if(counter_arr[i] < 100):
                    counter_arr[i] += 1
                    get_geo_data(station=station, base_request_string=base_request_string,
                               header=headers[i], result_arr=result_arr, fail_arr=fail_arr)
                    break
            sleep = all_equal(counterArr=counter_arr)
            if(sleep):
                print("<<<<<<<<<<<<<<SLEEPING>>>>>>>>>>>>>>>>", flush=True)
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


def send_on_success(record_metadata):
    print('topic:', record_metadata.topic,
          'partition:', record_metadata.partition)


# array with the important station data
stations = read_station("/home/bigdata/db-ripper/misc/Mappe1.csv")


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

compute_geo_data(stations, base_request_string, headers_arr,
               resultArr, failArr, counter_arr=counterArr)

with open("/home/bigdata/db-ripper/misc/test_table_result.csv", "w", newline='', encoding='utf-8') as resultfile:
    writer = csv.writer(resultfile)
    for result in resultArr:
        writer.writerow(result)
with open("/home/bigdata/db-ripper/misc/test_table_fail.csv", "w", newline='', encoding='utf-8') as failfile:
    writer = csv.writer(failfile)
    for fail in failArr:
        writer.writerow(fail)
