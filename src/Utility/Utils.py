from pathlib import Path

# ==================================
# API
# Invocation limit for timetable api per minute
timetableInvocationLimit = 20

# autorization token for DPApi
authToken1 = 'Bearer 13d747ec4d3615f93cca7dcf7f203389'
authToken2 = 'Bearer 873be58d3db312b4e52a2102e5641c27'

#authorization tokenlist for DPApi
tokenlistTimePark=["Bearer 66c1bf6e6d0ef593dd47424a4eec94bf", "Bearer 987e7819567c4d6ed7f583f28e0d70c2", "Bearer ee7c9ee9b982263124940af0cb510ae1",
"Bearer ec4341dce84961c2894fcb1227acf636", "Bearer 7549bcaf06c9b267ef5352e0e3c2adc5", "Bearer 153816852a4e1b6ac36a6fb9f6e00cdd",
"Bearer 3888f857751f4e7d6f40f3f656bba3a1", "Bearer 0824f9063615fe21fcf5738fb9199e02", "Bearer 787a1efb98cd211465927e7b709e33d4",
"Bearer 1170f1e14231e231920334eee13d0a0b", "Bearer bfcbc291384bd85009e71159c8e39973", "Bearer 3cc14ee5e64d83fd8b9feb431b11cb74",
"Bearer 2520cb358de74ba8b26e50585202b7b0", "Bearer 00b7d6a3ef63179362bfca99ba169074", "Bearer 8496a16924930326786469d05ee6eca2",
"Bearer 5c645e7a155ebf78d46ee8c50a47b094", "Bearer f9c7bcb19e98588409c8fcae9b0231d0", "Bearer e096f0e70e459f2edb62bfbec555ddd9",
"Bearer 41bc27751a3664dc93f8b7cc665c433e", "Bearer a18cd193ba76913782b2ee03d7e4454c", "Bearer c9b459b000f0546c588118ba3632582a",
"Bearer 8a4ea68ba9425a398ca520d4f9926b3b", "Bearer 8c1c4c46ddb0e526dae0e1f603eaef6a", "Bearer b405d4f521e7bd28bb444261454fb182",
"Bearer f8582df48a449ab6acab1f803e60d9f2", "Bearer e4c6ddc3adddd060675fbb9a33c65f12", "Bearer bdbc0654b5379a163e4ce058b61426c9",
"Bearer 4cec13292f68d70df50361196fac67b9", "Bearer 302e18c47877bdbc99a302b880188c2b", "Bearer 01bd007e8212570b40f70e4e183d9bcb",
"Bearer 4607434f378213f1202deb9d0e12e933", "Bearer 03e5ea3a47c2b96b28286590777aa870", "Bearer 75a98077afcdeb7ed9dd2e5432260b5a",
"Bearer 270e213be02ba3149958356e287782c6", "Bearer f062a085312f5436010076062aa2a90c", "Bearer 843213caece5ff579cd684106ea54ee5"]

tokenlistTimetable= ["Bearer 5002ccf110d8028b495e68b94b20f34b", "Bearer d7b4c9d087e81a13a7a1fb7051c6d97c", "Bearer 23024c042e9921fcb83e77f70298bf8c",
"Bearer 220b8035c458af332a3847229343c4c4", "Bearer c00ebb1cde4bdf257341bab8adb877f2", "Bearer ed62eff9f535e01944ce4ac712e28725",
"Bearer 7ee6366352a2bf6238692d80bcf056dd", "Bearer 84f5b3f6a6cd966d4f2553e204148232", "Bearer 18d200087d41fb10e39b47806828ded8",
"Bearer 9044d5f7d63323d83beac3484ea98b8f", "Bearer d450acd4a83fad43c670bd82b4ba2eb5", "Bearer b32d2b75006a81ef8d717d5d898f34c4", 
tokenlistTimePark]

tokenListTimeTablePlanned = ["Bearer 13d747ec4d3615f93cca7dcf7f203389","Bearer 873be58d3db312b4e52a2102e5641c27","Bearer 60ac42c7b66f64a502dd01996536ae3c",
"Bearer 5568f3ed56ef39b53d691101d314d3a2", "Bearer b2a8eea76d8a47f7c8163265c709e9fc", "Bearer 3460d2a98af6f3cf7913fc3c332ee2df"]

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
def get_changes_url(eva_number):
    return 'https://api.deutschebahn.com/timetables/v1/rchg/'+eva_number

# =======================================
# Kafka
# topic names
topicForPlannedTimetables = 'planned'
topicForChangedTimetabled = 'changed'
topicParkingTimetables = 'parking'
# kafka
bootstrap_servers = ['localhost:29092']

# timeInterval in seconds
planTimeInterval = 10
changeTimeInterval = 60

# =======================================
# config files
# csv file
def cityEvaRead():
    return open('../misc/table-1-result.csv', 'r')
def cityEvaWrite():
    return open('../misc/table-1-result.csv', 'w')
def parkingIDRead():
    return open('../misc/parking-allocations.csv', 'r')


# extract parking IDs out of csv file
def get_parking_IDs():
    csvfile = parkingIDRead
    parking_Ids = []
    for line in csvfile:
        try:
            lineArr = line.strip().split(",")
            parking_Ids.append(lineArr[0])
        except:
            print("Error in: extract parkingIDs from csv")

# exctract eva number out of the csv file
def get_eva_numbers():
    csvfile = cityEvaRead()
    eva_numbers = []
    for line in csvfile:
        try:
            lineArr = line.strip().split(",")
            eva_numbers.append(lineArr[1])
        except:
            print("Error in: extract eva-number from csv")
    return eva_numbers

# =====================================
# Elasticsearch
from elasticsearch import Elasticsearch

# default Index for elasticsearch
esIndex = 'timetable'

# connect with elasticsearch returns an elasticsearch_object
_es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
def connect_elasticsearch():
    return _es

# create Index on given elasticsearch_object, indexName can be given or use default
import threading
def synchronized(func):
	
    func.__lock__ = threading.Lock()
		
    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)

    return synced_func
@synchronized
def create_index(_es, index_name=esIndex):
    created = False
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }

    try:
        if not _es.indices.exists(index_name):
            _es.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created