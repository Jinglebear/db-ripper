import threading
from elasticsearch import Elasticsearch
from pathlib import Path

# ==================================
# API
# Invocation limit for timetable api per minute
timetableInvocationLimit = 20
weatherInvocationLimit = 60

# autorization token for DPApi
authToken1 = 'Bearer 13d747ec4d3615f93cca7dcf7f203389'
authToken2 = 'Bearer 873be58d3db312b4e52a2102e5641c27'

# authorization tokenlist for DPApi
tokenlistWaether = ["4d4e132b78899f18d0700d9786497acc", "f5dc99de2bd1e62827186cd0a59c969e", "83c4e7d593ba0a1557f8af1a0fe3e275",
                    "f80a8c8cf872626d896aaa5b1393ad0f", "db0a5045afe248adad429a54d3c32211", "c8160151557119cd85dbfbbb61f8c80a",
                    "2586b6e78610fa056868e8c2c903c7bc", "6224561d28db0ed922417fb668145c0f", "6224561d28db0ed922417fb668145c0f",
                    "ffeead68165685a1fe21c5b283821e69"]
tokenlistTimePark = ["Bearer 8101a6e392e28be9d112af1290bff9f0", "Bearer 31af1d273c3526061bd71031b3d16f5b", "Bearer c5b449234cafede57995910183a53e21",
                     "Bearer e1f0a27395765c17b20761760722dd6d", "Bearer 7d1dce5e52c3bf2a9c8f631b830b1297", "Bearer 80d56a23458e293c5a49d9bb355a1746",
                     "Bearer eb9d199b00759d463082ca02076e244a", "Bearer 3241a946c41f7506013f6898b9df9ad8", "Bearer 59a0a57c2126c08f273eb42d56b6777b",
                     "Bearer 8f44b5167eb4e90818e8404dafe95f3e", "Bearer 9882cf5ca945825fb1d0bcd9c109f7ea", "Bearer ef1971f5df9d3230146246aa6651b5ee", 
                     "Bearer 6be96be8e84c777bbf3e37cc7a66b2c0", "Bearer f24e13484afe12170cdab8a4d221a0f6", "Bearer c4ccc513c4f0215e002f7308fda985d1",
                     "Bearer db459857d9ba623c41d0a6a2027d0623", "Bearer 258b87eabf559a265226ae928f7d3f5b", "Bearer 898dcc297b93d8b709705e25b6bc11dd",
                     "Bearer a83724d60cc2fd975e051412256bcb14", "Bearer 7938e80b20a2b164c579e2d97b0080b7", "Bearer f52357ff26d62bd3dc05f5adfc7b4fcc",
                     "Bearer 59f1d306b254a205aed1d2927b785060", "Bearer aaf5b5abe2fd0ab93f7884147227641e", "Bearer 9dea8d679d8cb84ffd75895151512ab8",
                     "Bearer 753384652a723c362aa9ee7fefe3b60a", "Bearer 66848152f25e654ea59d4254290efc4e", "Bearer c9a22b053d10a34569926f973c846cdf",
                     "Bearer 713991fa9b97cb58cc9cd74f7346dabf", "Bearer 1cfb92d32ad67b96687323e15fdf5833", "Bearer 17339820e735c7a0851b3f703a2458d9",
                     "Bearer acf85b3a94052f9b2a8ae776e2a19d9b", "Bearer 586e174f676d75653dd27ac3a6a030a6", "Bearer 57338971976d340c5f0d39dadc6f49f5",
                     "Bearer c99f6b32dc932c61bdd015608f5c6c28", "Bearer e8c955165efb96d84a67ef518961a1f7", "Bearer 02467e74866677b2e245fb5ef1c6d83d"]

tokenlistTimetable = ["Bearer 5002ccf110d8028b495e68b94b20f34b", "Bearer d7b4c9d087e81a13a7a1fb7051c6d97c", "Bearer 23024c042e9921fcb83e77f70298bf8c",
                      "Bearer 220b8035c458af332a3847229343c4c4", "Bearer c00ebb1cde4bdf257341bab8adb877f2", "Bearer ed62eff9f535e01944ce4ac712e28725",
                      "Bearer 7ee6366352a2bf6238692d80bcf056dd", "Bearer 84f5b3f6a6cd966d4f2553e204148232", "Bearer 18d200087d41fb10e39b47806828ded8",
                      "Bearer 9044d5f7d63323d83beac3484ea98b8f", "Bearer d450acd4a83fad43c670bd82b4ba2eb5", "Bearer b32d2b75006a81ef8d717d5d898f34c4",
                      "Bearer 91e5005e763756a5cbf10f08b70be49f", "Bearer cd4e75acf162aded388fac9b33d47b8e", "Bearer cf7206e70b3a676378537b3f10fab140",
                      "Bearer 9e432a5c44e30d8d99879289b816f54a", "Bearer 1d95b3eeeb1c3b43b3a93d4d79efe8a8", "Bearer e0da58c61723f1f1e46b9277a4ab19f7",
                      "Bearer a232ac0c7405229aab90cba202933733", "Bearer 034255b708bcf4999d24ef68320ad5bc", "Bearer 83efe2743608bbe15acc89ec5bf76559",
                      "Bearer dae86c9e2f0831e23ed792f842b06dee", "Bearer 68926b817f10d72896dec1a3429b3569", "Bearer f5f89a18a98160c4cb7072ab2e36dcea"
                      "Bearer edba688f58d3a47c716b32080b2ea2ed", "Bearer 3cd12d8d9c769aa2f9ebe426a3f9d7fc"] + (tokenlistTimePark)

tokenListTimeTablePlanned = ["Bearer 13d747ec4d3615f93cca7dcf7f203389", "Bearer 873be58d3db312b4e52a2102e5641c27", "Bearer 60ac42c7b66f64a502dd01996536ae3c",
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
# KafkaProducerParking
# Header for Bahn Park API request
ParkingHeader = headers = {'Accept': 'application/json;charset=utf-8',
                           'Authorization': 'Bearer eed6fa376f86dce95beb400315616e58'
                           }
# api urls
def get_parking_url():
    return "https://api.deutschebahn.com/bahnpark/v1/spaces/occupancies"

def get_planned_url(eva_number, date, hour_slice):
    if len(hour_slice) == 1:
        hour_slice = "0" + hour_slice
    return 'https://api.deutschebahn.com/timetables/v1/plan/'+eva_number+'/'+date+'/'+hour_slice


def get_changes_url(eva_number):
    return 'https://api.deutschebahn.com/timetables/v1/rchg/'+eva_number


def get_weather_url(cityName, apiKey):
    return 'https://api.openweathermap.org/data/2.5/weather?q='+cityName+'&appid='+apiKey


# =======================================
# Kafka
# topic names
topicForPlannedTimetables = 'planned'
topicForChangedTimetabled = 'changed'
topicParkingTimetables = 'parking'
topicWeather = 'weather'
# kafka
bootstrap_servers = ['localhost:29092']

# timeInterval in seconds
planTimeInterval = 3600
changeTimeInterval = 60
weatherTimeInterval = 3600

# =======================================
# config files
# csv file


def cityEvaRead():
    return open('/home/johannes/db-ripper/misc/table-2-sorted(category4).csv', 'r')


def cityEvaWrite():
    return open('../misc/table-1-result.csv', 'w')


def parkingIDRead():
    return open('../misc/parking-allocations.csv', 'r')

def cityNameRead():
    return open('../misc/table-2-sorted(category4).csv','r')


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
# extract cityName out of the csv file


def get_cityName():
    csvfile = cityEvaRead()
    cityNames = []
    for line in csvfile:
        try:
            lineArr = line.strip().split(",")
            cityNames.append(lineArr[0])
        except:
            print("Error in: extract cityNames from csv")
    return cityNames
    
#extract cityName out of the csv file for WeatherApi
def get_cityName_Weather():
    csvfile=cityNameRead()
    cityNames=[]
    for line in csvfile:
        try:
            lineArr=line.strip().split(",")
            if lineArr[3] not in cityNames:
                cityNames.append(lineArr[3])
        except:
            print("Error in: extract cityNames for Weather from csv")

# =====================================
# Elasticsearch


# default Index for elasticsearch
esIndex = 'timetable'

# connect with elasticsearch returns an elasticsearch_object
_es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


def connect_elasticsearch():
    return _es


# create Index on given elasticsearch_object, indexName can be given or use default


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
