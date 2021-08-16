import Utils

es = Utils.connect_elasticsearch()
Utils.create_index(es)

query = {"query":{"match":{"id":{"query":"5813921332645815579-2108020739-12","operator": "and"}}}}
object = es.search(index=Utils.es_default_index, body=query)

print(object['hits']['hits'][0]['_source'])