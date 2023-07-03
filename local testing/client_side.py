import pika
import uuid
import time
import json

def on_reply_message_received(ch, method, properties, body):
    v = json.loads(body)
    print(v)
    channel.stop_consuming()

connection_parameters = pika.ConnectionParameters('localhost')

connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel()

reply_queue = channel.queue_declare(queue='', exclusive=True)

channel.basic_consume(queue=reply_queue.method.queue, auto_ack=True,
    on_message_callback=on_reply_message_received)

channel.queue_declare(queue='request-queue')

cor_id = str(uuid.uuid4())
print(f"Sending Request: {cor_id}")

# json_object = {
# "id": [
# "iisc.ac.in/89a36273d77dac4cf38114fca1bbe64392547f86/rs.iudx.io/pune-env-flood/FWR053"
# ],
# "attr-query": "isValid==%7B%0A%22departmentid%22%3A%20%2222%22%2C%0A%22SRNID%22%3A%20%226958961347%22%2C%0A%22FName%22%3A%20%22Shailesh%22%2C%0A%22MName%22%3A%20%22Bhisham%22%2C%0A%22LName%22%3A%20%22J%22%2C%0A%22DOB%22%3A%20%221997-08-07%22%2C%0A%22Gender%22%3A%20%22M%22%2C%0A%22FatherHusbandName%22%3A%20%22Bhisham%22%2C%0A%22RationCardFamilyNo%22%3A%20%22226236000000%22%2C%0A%22RationCardMemberNo%22%3A%20%224%22%2C%0A%22MobileNumber%22%3A%20%227689586951%22%2C%0A%22EmailID%22%3A%20%22test%40gmail.com%22%2C%0A%22BankAccounNo%22%3A%20%22234567892%22%2C%0A%22IFSCCode%22%3A%20%22BKID0000806%22%2C%0A%22HouseBuildingApartmentNumber%22%3A%20%22S-11%22%2C%0A%22StreetRoadLane%22%3A%20%22Nehru%20nagar%22%2C%0A%22Landmark%22%3A%20%22Nehru%20nagar%22%2C%0A%22AreaLocalitySector%22%3A%20%22Bilaspur%22%2C%0A%22VillageTownCity%22%3A%20%22Bilaspur%22%2C%0A%22SubDistrictCode%22%3A%20%22752%22%2C%0A%22DistrictCode%22%3A%20%22545%22%2C%0A%22StateCode%22%3A%20%22300%22%2C%0A%22PinCode%22%3A%20%22491771%22%2C%0A%22LocationAddress%22%3A%20%22Bilaspur%22%2C%0A%22UrbanRural%22%3A%20%22Urban%22%2C%0A%22UrbanLocalBodyGramPanchayat%22%3A%20%22neharu%20nagar%22%2C%0A%22WardVillage%22%3A%20%226%22%0A%7D;departmentid==22",
# "searchType": "latestSearch_attributeSearch",
# "instanceID": "localhost:8443",
# "applicableFilters": [
# "ATTR",
# "TEMPORAL"
# ],
# "publicKey": None
# }

# json_object= {
# "id": [
# "iisc.ac.in/89a36273d77dac4cf38114fca1bbe64392547f86/rs.iudx.io/pune-env-flood/FWR055"
# ],
# "temporal-query": {
# "time": "2021-06-09T14:20:00Z",
# "endtime": "2021-06-10T14:20:00Z",
# "timerel": "during"
# },
# "attr-query": "year==2020-2021;schemecode==22",
# "searchType": "temporalSearch_attributeSearch",
# "instanceID": "localhost:8443",
# "applicableFilters": [
# "ATTR",
# "TEMPORAL"
# ],
# "publicKey": None
# }

# json_object={
#     "id": [
#     "iisc.ac.in/89a36273d77dac4cf38114fca1bbe64392547f86/rs.iudx.io/pune-env-flood/FWR055"
#     ],
#     "attr-query": "villcode==2601001;surveyno==48ఉ1",
#     "searchType": "temporalSearch_attributeSearch",
#     "instanceID": "localhost:8443",
#     "applicableFilters": [
#     "ATTR",
#     "TEMPORAL"
#     ],
#     "publicKey": None,
#     "ppbNumber":"T26010010910"

# }

# json_object= {
# "id": [
# "iisc.ac.in/89a36273d77dac4cf38114fca1bbe64392547f86/rs.iudx.io/pune-env-flood/FWR055"
# ],
# "temporal-query": {
# "time": "2022-08-22T12:01:05Z",
# "endtime": "2022-08-22T12:01:05Z",
# "timerel": "during"
# },
# "attr-query": "Finyear==2022;Season==2;DistCode==12;MandCode==248;VillCode==991211003;ppbnumber==T13010130530",
# "searchType": "temporalSearch_attributeSearch",
# "instanceID": "localhost:8443",
# "applicableFilters": [
# "ATTR",
# "TEMPORAL"
# ],
# "publicKey": None,
# "ppbNumber":"T13010130530"
# }



# json_object = {
# 	"id": [
# 		"rythubandhu.telangana.gov.in/1668984090c7e92e4c39e1e5d29300e6a9b2a11e/gateway.adex.org.in/rythubandhu-schemes/get-cb-data"
# 	],
# 	"temporal-query": {
# 		"time": "2022-08-22T12:01:05Z",
# 		"endtime": "2022-08-22T110:05Z",
# 		"timerel": "during"
# 	},
# 	"attr-query": "Finyear==2022;Season==1;DistCode==26;MandCode==480;VillCode==2601001",
# 	"searchType": "temporalSearch_attributeSearch",
# 	"instanceID": "localhost:8443",
# 	"applicableFilters": [
# 		"ATTR",
# 		"TEMPORAL"
# 	],
# 	"publicKey": "None",
# 	"ppbNumber": "T13010130530"
# }

json_object = {
	"id": [
		"rythubandhu.telangana.gov.in/1668984090c7e92e4c39e1e5d29300e6a9b2a11e/gateway.adex.org.in/rythubandhu-schemes/get-cb-data"
	],
	"temporal-query": {
		"time": "2021-01-25T12:01:05Z",
		"endtime": "2023-05-01T12:10:05Z",
		"timerel": "during"
	},
	"attr-query": "Ppbno==T01050090085",
	"searchType": "temporalSearch_attributeSearch",
	"instanceID": "localhost:8443",
	"applicableFilters": [
		"ATTR",
		"TEMPORAL"
	],
	"publicKey": "None",
	"ppbNumber": "T13010130530"
}

# json_object={
#    "id":[
#       "datakaveri.org/5841d4c98a9ef8ab708e01a3c5fc1ab955e6ada0/rs.adex.org.in/khammam-crop-test/mandal-crop-info-test1"
#    ],
#    "attr-query":"ppbnumber==T13010130530",
#    "searchType":"latestSearch_attributeSearch",
#    "instanceID":"localhost:8081",
#    "applicableFilters":[
      
#    ],
#    "publicKey":"",
#    "ppbNumber":"T13010130530"
# }

message = json.dumps(json_object)


channel.basic_publish('', routing_key='request-queue', properties=pika.BasicProperties(
    reply_to=reply_queue.method.queue,
    correlation_id=cor_id
), body=message)

print("Starting Client")

channel.start_consuming()

