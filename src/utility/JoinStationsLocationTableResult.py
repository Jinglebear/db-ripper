import csv

# ====================================================================
# **Description**
# This script takes the ~1115 train stations containing :
# --> train station name 
# --> evaluation number ( train station identifier in db-api)
# --> train station category (1-4, 1 means biggest train station)
# --> name of the city the train station is placed in
# and enriches it with the geoJSON (Point) by joining it with location table

# read 1115 train stations
def read_eva_table(filename):
    stations=[]
    with open(file=filename, encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            #first entry == station name
            #second entry == eva number
            #third entry == Category 
            #fourth entry == city name
            station = (row[0],row[1],row[2],row[3])
            #add station to the array of stations
            stations.append(station)
    return stations

# function call
stationsEva = read_eva_table("..\\..\\misc\\table-2-sorted(category4).csv")

# read location table
def read_location_table(filename):
    stations=[]
    with open(file=filename, encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            #first entry == station name
            #second entry == Category
            #third entry ==  city name
            #fourth entry == city location
            station = (row[0],row[1],row[2],row[3])
            #add station to the array of stations
            stations.append(station)
    return stations

#function call
stationsLocation = read_location_table("..\\..\\misc\\table_stations_location_final.csv")
# joined data array
stationsJoined = []

# join data
for stationEva in stationsEva:
    for stationLocation in stationsLocation:
        if(stationEva[0] == stationLocation[0]):
            #0 = station name, 1 = evaNumber, 2= station category, 3= station city name, 4= station city location
            station = (stationEva[0],stationEva[1],stationEva[2],stationEva[3],stationLocation[3])
            stationsJoined.append(station)

#filter out duplicates
stationsJoinedNoDpS=[]
for station in stationsJoined:
    if station in stationsJoinedNoDpS:
        continue
    else:
        stationsJoinedNoDpS.append(station)

#write result in new file joinedTables.csv
with open("..\\..\\misc\\joined_tables.csv","w",newline='',encoding='utf-8')as outputfile:
    writer = csv.writer(outputfile)
    for station in stationsJoinedNoDpS:
        writer.writerow(station)