import datetime
import json
import logging
import sys
import urllib.error
from configparser import ConfigParser
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import requests
import re
import pika
import xmltodict
from dateutil import parser

config = ConfigParser(interpolation=None)
config.read("../config/config_file.ini")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
time_format = "%Y-%m-%dT%H:%M:%SZ"
time_formatter = "%Y-%m-%dT%H:%M:%S.%f"

class RabbitMqServerConfigure():

    def __init__(self,host,queue):

        """ Server initialization   """


        self.host = host
        self.queue = queue

class rabbitmqServer(object):

    def __init__(self, server):

        """
        Establishing the connection
        :param server: Object of class RabbitMqServerConfigure
        :return:None
        """

        self.server = server 


        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.server.host
                )
            )

        self.channel = self.connection.channel()

        print("Server started waiting for Messages ")
        
    def startserver(self, on_request):

        """
        Starting the server to consume
        :param on_reuest:callback
        :return:None
        """
        # self.channel.basic_qos(prefetch_count=1)

        self.channel.basic_consume(
            queue=self.server.queue,
            on_message_callback=on_request,
            auto_ack=True
            )

        self.channel.start_consuming()  
        
        
    def publish(self, payload, rout_key, corr_id, method):

        """
        :param payload: JSON payload
        :param rout_key: reply queue
        :param corr_id: correlation id 
        :return: None
         """

        message = json.dumps({"message": payload, "routing_key":rout_key, "correlation_id":corr_id})
        self.channel.basic_publish(
            exchange='', 
            routing_key=rout_key, 
            properties=pika.BasicProperties(correlation_id = corr_id),
            body=message
            )
        # self.channel.basic_ack(delivery_tag=method.delivery_tag)

        print(f"Published Message: {payload}")

class krishi_tantra_data:

    def __init__(self, url, queue):

        """ Variable initialization   """

        self.url = url
        self.queue = queue

    def process_request(self, ch, method, properties, body):

        """
        Processing the request
        :param method: 
        :param properties: properties consists of required ids
        :param body: request json response to form the api
        :return:None
        """

        logging.info(".......RequestJson received......")
        self.json_object = json.loads(body)
        self.rout_key=properties.reply_to
        self.corr_id = properties.correlation_id
        self.method = method
        
        logging.info(".........Forming the api.........")

        self.getToken()

    def getToken(self):
        
        try:
            
            payload = """{
                "query":"mutation GenerateAccessToken($refreshToken: String!, $clientId: String!, $clientSecret: String!) {generateAccessToken(refreshToken: $refresToken, client_id: $clientId, client_secret: $clientSecret)}",
                "variables":{"refreshToken":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlblR5cGUiOiJSZWZyZXNoVG9rZW4iLCJvcmdhbml6YXRpb24iOiI2Mjk2ZmI4MTJiY2EzZjAwMTNhMzFjYWYiLCJ0eXBlIjoiVXNlciIsInN1YiI6IjYzZTllYzNlZTBjOGY1MDAxOTA5N2FjYSIsImlhdCI6MTY4NDIzODg2NSwiZXhwIjoxOTk5ODE0ODY1LCJhdWQiOiJLcmlzaGl0YW50cmEuY29tIiwiaXNzIjoiS3Jpc2hpdGFudHJhLmNvbSIsImp0aSI6IjcxMzk0NmY0LWQ3ZjgtNDE2OC05ZDhkLWU5NzlmYjE1NmQxOSJ9.aCNaBQc66nVtNiQphsBxfQHMmPvHPd8Z9SP_K1cFXgo\",\"clientId\":\"6463720eb77653001be002f1\",\"clientSecret\":\"fc*uL$wCRe5gkmT&u2S&tN4TJTv6#ZD6"}
                }"""
            headers = {
            'Content-Type': 'application/json'
            }

            response = requests.request("POST", self.url, headers=headers, data=payload)
            
            logging.info(".........fetching the token.........")

            token = response.json()["data"]["generateAccessToken"]["token"]
            
            self.getData(token)

            
        except urllib.error.HTTPError as eh:

            logging.error(f"An Http Error occurred: {eh}")

            dictionary["statusCode"] = eh.code
            dictionary["details"] =  eh.reason

        except urllib.error.URLError as eu:

            logging.error(f"An URL Error occurred: {eu}")

            dictionary["statusCode"] = eu.reason.args[0]
            dictionary["details"] =  str(eu.reason)
            
        except KeyError as ek:

            logging.error(f"A Key error occurred: {ek}")

            error_dict["statusCode"] = 400
            error_dict["details"] = f"Keyerror: Key {str(ek)} not found in the json request body"


        except Exception as e:

            logging.error(f"An Unknown Error occurred: {e}")

            dictionary["statusCode"] = 400
            dictionary["details"] = 'An unknown error occurred while processing the request on the server'

    def getData(self, token):

        """
        Fetching from seeding api
        :params end_point: end point which consists of attribute or temporal query
        :return None:
        """

        dictionary ={}

        try:
            payload = "{\"query\":\"query GetTestByOrg($computedId: String, $testCentre: String, $from: String, $to: String) {\\n  getTestByOrg(computedID: $computedId, testCentre: $testCentre, from: $from, to: $to) {\\n     computedID\\n    id\\n    location\\n    crop\\n    previousCrop\\n    previousYield\\n    previousYieldPrice\\n    targetYield\\n    previousManure\\n    previousfertilizerQuantity\\n     results\\n    testCentre {\\n      id\\n      name\\n      createdAt\\n      updatedAt\\n      testCentreCode\\n    }\\n    plot {\\n        surveyNo\\n    }\\n    createdAt\\n    updatedAt\\n    startedAt\\n    endedAt\\n    sampleDate\\n    testCompletedAt\\n  }\\n}\",\"variables\":{\"testCentre\":\"digitalgreen00001\"}}"

            headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer {}'.format(token)
            }

            req = Request(self.url, payload.encode('utf-8'), headers=headers )
            
            response = urlopen(req)
            
            logging.info(".........got the response.........")

            status = response.getcode()

            dictionary = self.fetch_response(status, response, dictionary)

        except urllib.error.HTTPError as eh:

            logging.error(f"An Http Error occurred: {eh}")

            dictionary["statusCode"] = eh.code
            dictionary["details"] =  eh.reason

        except urllib.error.URLError as eu:

            logging.error(f"An URL Error occurred: {eu}")

            dictionary["statusCode"] = eu.reason.args[0]
            dictionary["details"] =  str(eu.reason)

        except Exception as e:

            logging.error(f"An Unknown Error occurred: {e}")

            dictionary["statusCode"] = 400
            dictionary["details"] = 'An unknown error occurred while processing the request on the server'

        server.publish(dictionary, self.rout_key, self.corr_id, self.method)

    def fetch_response(self, status, response, dictionary):
        
        """
        Fetches the response and status code from the API
        :param status: Status of the response
        :param response: Response from the API
        :param dictionary: Dictionary to store the response data
        :return: Updated dictionary with status and results/details
        """
        resp= json.loads(response.read())
        
        if status==200:
            
            dictionary['statusCode'] = status
            json_array = resp["data"]["getTestByOrg"]
            dictionary["results"] = json_array 
            
        else:
            
            dictionary['statusCode'] = status
            dictionary["details"] = resp 

        return dictionary

if __name__ == '__main__':


    username = config["server_setup"]["username"]
    password = config["server_setup"]["password"]
    host = config["server_setup"]["host"]
    port = config["server_setup"]["port"]
    vhost = config["server_setup"]["vhost"]
    queue = config["krishi_tantra_queue"]["queue"]
    url = config["krishi_tantra_url"]["url"]

    ktd = krishi_tantra_data(url, queue)
    serverconfigure = RabbitMqServerConfigure( username, password, host, port, vhost, queue)
    server = rabbitmqServer(server=serverconfigure)
    server.startserver(ktd.process_request)
