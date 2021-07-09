import csv
from os import write
import requests
import json

### Auth header and API  URL 
request_string = "https://api.deutschebahn.com/bahnpark/v1/spaces/occupancies"
header = {
    'Accept': 'application/json;charset=utf-8',
    'Authorization': 'Bearer eed6fa376f86dce95beb400315616e58'
}

### getParkingAllocations function
### calls DB API (Bahnpark)
### returns tupel of (Allocation ID, Allocation Parkhouse Name, Allocation Station Name, Allocation Capacity, Allocation Category)
def getParkingAllocations(stations):
    response = requests.get(request_string,headers=header)

    response_status_code = response.status_code
    stations = []
    if(response_status_code == 200):
        print("success")
        response = response.json()
        #data = json.loads(response)
        allocations =response['allocations']
        for allocation in allocations:
            allocation_id = allocation['space']['id']
            allocation_parkhouse_name = allocation['space']['name']
            allocation_station_name = allocation['space']['station']['name']
            allocation_capacity = allocation["allocation"]["capacity"]
            if(len(allocation["allocation"]) == 6):
                allocation_category = allocation["allocation"]["category"]
                stations.append((allocation_id,allocation_parkhouse_name,allocation_station_name,allocation_capacity,allocation_category))
    else:
        print ("Wrong status code: %d" % (response_status_code))
    return stations


### writeResult function
### writes the result of getParkingAllocations() into a csv file
def writeResult(stations):
    with open("..\\misc\\parking-allocations.csv","w",newline='',encoding='utf-8') as parking_allocations:
        writer = csv.writer(parking_allocations)
        first_line = (("Allocation ID"),("Allocation Parkhouse Name"),("Allocation Station Name"),("Allocation Capacity"),("Allocation Category"))
        writer.writerow(first_line)
        for station in stations:
            writer.writerow(station)
