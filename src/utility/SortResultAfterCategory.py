import csv

# ====================================================================
# **Description**
# This scripts takes a list of train stations and filters out all train
# stations that have a category of 5 and higher
# results are being written to a file
# -->table-1-sorted(category4).csv

# read in the stations
def read_station(filename):
    stations = []
    with open(filename, encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            station_triplet = (row[0], row[1], row[2])
            stations.append(station_triplet)
    return stations
stations = read_station("..\\misc\\table-1-result.csv")

# sort out all stations with cat 5 or higher
# write result
with open("..\\misc\\table-1-sorted(category4).csv","w",newline='',encoding='utf-8')as outputfile:
    writer = csv.writer(outputfile)
    for station in stations:
        tmp = station[2]
        if(tmp == '1' or tmp == '2' or tmp == '3' or tmp == '4'):
            writer.writerow(station)
