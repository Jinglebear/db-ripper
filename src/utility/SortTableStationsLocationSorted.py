import csv

# ====================================================================
# **Description**
# This scripts takes a list of train stations and sorts it with the 
# python sort() method

def read_station(filename):
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
stations = read_station("..\\..\\misc\\table_stations_location_sorted.csv")

stations.sort()

with open("..\\..\\misc\\table_stations_location_final.csv","w",newline='',encoding='utf-8')as outputfile:
    writer = csv.writer(outputfile)
    for station in stations:
        writer.writerow(station)