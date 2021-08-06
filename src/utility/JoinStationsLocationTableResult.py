import csv
def readEvaTable(filename):
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
stationsEva = readEvaTable("..\\..\\misc\\table-2-sorted(category4).csv")


def readLocationTable(filename):
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
stationsLocation = readLocationTable("..\\..\\misc\\table_stations_location_final.csv")
stationsJoined = []

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

####
#write result in new file joinedTables.csv



with open("..\\..\\misc\\joined_tables.csv","w",newline='',encoding='utf-8')as outputfile:
    writer = csv.writer(outputfile)
    for station in stationsJoinedNoDpS:
        writer.writerow(station)