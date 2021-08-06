import csv
from os import read, write
def readStation(filename):
    stations=[]
    with open(file=filename, encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            #first entry == station name
            #second entry == station category
            #third entry == station city name 
            #fourth entry == city location
            station = (row[0],row[1],row[2],row[3])
            #add station to the array of stations
            stations.append(station)
    return stations
stations = readStation("..\\..\\misc\\test_table_result.csv")

with open("..\\..\\misc\\table_stations_location_sorted.csv","w",newline='',encoding='utf-8')as outputfile:
    writer = csv.writer(outputfile)
    for station in stations:
        #get category for every entry
        try:
            category = int(station[1])
            #check if it is in range 1 - 4
            #(this is a string)
            if category <= 4:
                writer.writerow(station)
        except Exception as e:
            print(e)
            print(station)
