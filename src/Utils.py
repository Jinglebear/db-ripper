from pathlib import Path

# autorization token for DPApi
authToken1 = 'Bearer 13d747ec4d3615f93cca7dcf7f203389'
authToken2 = 'Bearer 873be58d3db312b4e52a2102e5641c27'

# curl header
TimeTableHeader1 = headers = {
    'Accept': 'application/xml',
    'Authorization': authToken1,
}
TimeTableHeader2 = headers = {
    'Accept': 'application/xml',
    'Authorization': authToken2,
}

# api urls
def get_planned_url(eva_number, date, hour_slice):
    return 'https://api.deutschebahn.com/timetables/v1/plan/'+eva_number+'/'+date+'/'+hour_slice

# topic names
topicForPlannedTimetables = 'planned'
topicForChangedTimetabled = 'changed'

# kafka
bootstrap_servers = ['localhost:29092']

# timeInterval in seconds
planTimeInterval = 3600
changeTimeInterval = 60

# file
# csv file
cityEvaRead = open('../misc/table-1-result.csv', 'r')
cityEvaWrite = open('../misc/table-1-result.csv', 'w')