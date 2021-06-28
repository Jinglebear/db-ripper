from pathlib import Path

# autorization token for DPApi
authToken1 = 'Bearer 13d747ec4d3615f93cca7dcf7f203389'
authToken2 = 'Bearer 873be58d3db312b4e52a2102e5641c27'

#authorization tokenlist for DPApi
tokenlist= ["Bearer 13d747ec4d3615f93cca7dcf7f203389","Bearer 873be58d3db312b4e52a2102e5641c27","Bearer 60ac42c7b66f64a502dd01996536ae3c",
"Bearer 5568f3ed56ef39b53d691101d314d3a2", "Bearer b2a8eea76d8a47f7c8163265c709e9fc", "Bearer 3460d2a98af6f3cf7913fc3c332ee2df",
"Bearer 5002ccf110d8028b495e68b94b20f34b", "Bearer d7b4c9d087e81a13a7a1fb7051c6d97c", "Bearer 23024c042e9921fcb83e77f70298bf8c",
"Bearer 220b8035c458af332a3847229343c4c4", "Bearer c00ebb1cde4bdf257341bab8adb877f2", "Bearer ed62eff9f535e01944ce4ac712e28725",
"Bearer 7ee6366352a2bf6238692d80bcf056dd", "Bearer 84f5b3f6a6cd966d4f2553e204148232", "Bearer 18d200087d41fb10e39b47806828ded8",
"Bearer 9044d5f7d63323d83beac3484ea98b8f", "Bearer d450acd4a83fad43c670bd82b4ba2eb5", "Bearer b32d2b75006a81ef8d717d5d898f34c4" ]

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
planTimeInterval = 10
changeTimeInterval = 60

# file
# csv file
def cityEvaRead():
    return open('../misc/table-1-result.csv', 'r')
def cityEvaWrite():
    return open('../misc/table-1-result.csv', 'w')