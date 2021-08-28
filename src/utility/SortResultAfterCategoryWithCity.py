import csv

# ====================================================================
# **Description**
# This scripts takes a list of train stations and filters out all train
# stations that have a category of 5 and higher
# results are being written to a file

def read_station(filename):
    stations = []
    with open(filename, encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            station_quadruplet = (row[0], row[1], row[2], row[3])
            stations.append(station_quadruplet)
    return stations
stations = read_station("..\\..\\misc\\table-2-result.csv")

with open("..\\..\\misc\\table-2-sorted(category4).csv","w",newline='',encoding='utf-8')as outputfile:
    writer = csv.writer(outputfile)
    for station in stations:
        tmp = station[2]
        if(tmp == '1' or tmp == '2' or tmp == '3' or tmp == '4'):
            writer.writerow(station)
