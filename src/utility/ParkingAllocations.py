import csv
from os import write
import requests
import json

# ==========================================================================================
# **Description**
# This script calls the db-API 'Parking' to gather the data of monitored parking allocations
# --> Allocation ID
# --> Allocation Parkhouse Name
# --> Allocation Train Station Name
# --> Allocation Capacity
# --> Allocation Category :
#       ( 1 = up to 10 places are free )
#       ( 2 = more than 10 places are free )
#       ( 3 = more than 30 places are free )
#       ( 1 = more than 50 places are free )

### Auth header and API  URL 
request_string = "https://api.deutschebahn.com/bahnpark/v1/spaces/occupancies"
header = {
    'Accept': 'application/json;charset=utf-8',
    'Authorization': 'Bearer eed6fa376f86dce95beb400315616e58'
}

### get_parking_allocations function
### calls DB API (Bahnpark)
### returns tupel of (Allocation ID, Allocation Parkhouse Name, Allocation Station Name, Allocation Capacity, Allocation Category)
def get_parking_allocations():
    response = requests.get(request_string,headers=header)

    response_status_code = response.status_code
    stations = []
    if(response_status_code == 200):
        print("success")
        response = response.json()
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


### write_result function
### writes the result of get_parking_allocations() into a csv file
def write_result(stations):
    with open("..\\misc\\parking-allocations.csv","w",newline='',encoding='utf-8') as parking_allocations:
        writer = csv.writer(parking_allocations)
        first_line = (("Allocation ID"),("Allocation Parkhouse Name"),("Allocation Station Name"),("Allocation Capacity"),("Allocation Category"))
        writer.writerow(first_line)
        for station in stations:
            writer.writerow(station)
