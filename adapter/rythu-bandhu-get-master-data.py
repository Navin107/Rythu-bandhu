import json
import logging
import pika
import sys
import urllib.error
from configparser import ConfigParser
from urllib.request import Request, urlopen
from urllib.parse import urlparse
import xmltodict

config = ConfigParser(interpolation=None)
config.read("../config/config_file.ini")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class RabbitMqServerConfigure():

    def __init__(self, username, password, host, port, vhost, queue):

        """ Server initialization   """

        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.vhost = vhost
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
            pika.URLParameters(f'amqps://{self.server.username}:{self.server.password}@{self.server.host}:{self.server.port}/{self.server.vhost}'))

        self.channel = self.connection.channel()
        
        logging.info("......Server started waiting for Messages......")
        
    def startserver(self, on_request):

        """
        Starting the server to consume
        :param on_reuest:callback
        :return:None
        """
        self.channel.basic_qos(prefetch_count=1)

        self.channel.basic_consume(
            queue=self.server.queue,
            on_message_callback=on_request
            )

        self.channel.start_consuming()  
        
        
    def publish(self, payload, rout_key, corr_id, method):

        """
        :param payload: JSON payload
        :param rout_key: reply queue
        :param corr_id: correlation id 
        :return: None
        """

        message = json.dumps(payload)
        
        self.channel.basic_publish(
            exchange='', 
            routing_key=rout_key, 
            properties=pika.BasicProperties(correlation_id = corr_id),
            body=message
            )

        print(message)
        
        self.channel.basic_ack(delivery_tag=method.delivery_tag)

        logging.info(".......Message is published.......")

class master_data:

    def __init__(self, url, queue, iudx_username, iudx_password):

        """ Variable initialization   """

        self.url = url
        self.queue = queue
        self.iudx_username = iudx_username
        self.iudx_password = iudx_password

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

        self.form_api()

    def form_api(self):
        
        """
        Forming the API
        :return:None
        """
        error_dict = {}
        
        try:
            
            self.getData()
            return

        except KeyError as ek:

            logging.error("A Key error occurred: {}".format(ek))

            error_dict["status"] = 400
            error_dict["details"] = f"Keyerror: Key {str(ek)} not found in the json request body"

        except Exception as e:  

            logging.error("An Unknown Error occurred: {}".format(e))

            error_dict["status"] = 400
            error_dict["details"] = str(e)

        server.publish(error_dict, self.rout_key, self.corr_id, self.method) 

    def getData(self):

        """
        Fetching from seeding api
        :params end_point: end point which consists of attribute or temporal query
        :return None:
        """

        dictionary ={}

        try:

            url = self.url                
            payload =  """<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                            <soap:Body>
                                <Get_RB_Master_Data xmlns="http://tempuri.org/">
                                <WS_UserName>{}</WS_UserName>
                                <WS_Password>{}</WS_Password>
                                </Get_RB_Master_Data>
                            </soap:Body>
                        </soap:Envelope>""".format(self.iudx_username, self.iudx_password)
            
            headers = {
                'Content-Type': 'text/xml'
                }

            req = Request(url, payload.encode('utf-8'), headers=headers )
            response = urlopen(req)
            status = response.getcode()

            dictionary = self.fetch_response(status, response, dictionary)

        except urllib.error.HTTPError as eh:
            
            logging.error("An Http Error occurred: {}".format(eh))
            
            dictionary["statusCode"] = eh.code 
            dictionary["details"] =  eh.reason
            
        except urllib.error.URLError as eu:
            
            logging.error("An URL Error occurred: {}".format(eu))
            
            dictionary["statusCode"] = eu.reason.args[0]
            dictionary["details"] =  str(eu.reason)

        except Exception as e:
            
            logging.error("An Unknown Error occurred: {}".format(e))
            
            dictionary["statusCode"] = 400
            dictionary["details"] = 'An unknown error occurred while processing the request on the server'


        server.publish(dictionary, self.rout_key, self.corr_id, self.method)

    def fetch_response(self, status, response, dictionary):
        
        """
        Fetching the response and status code from URL
        :params status: Status of the response
        :params response: response from the api
        :return dictionary: dictionary which consists of status and results/details
        """
        
        resp_dict = xmltodict.parse(response.read())

        if status==200:
            soap = resp_dict["soap:Envelope"]["soap:Body"]

            response_json = json.loads(soap["Get_RB_Master_DataResponse"]["Get_RB_Master_DataResult"])
            success_flag = response_json["SuccessFlag"]
            success_msg = response_json["SuccessMsg"]

            if success_flag == "1":
                dictionary['statusCode'] =  status
                dictionary["results"] = response_json["Data"]

            else:
                dictionary['statusCode'] =  204
                dictionary["details"] = success_msg

        return dictionary

if __name__ == '__main__':


    username = config["server_setup"]["username"]
    password = config["server_setup"]["password"]
    host = config["server_setup"]["host"]
    port = config["server_setup"]["port"]
    vhost = config["server_setup"]["vhost"]
    queue = config["master_data_queue"]["queue"]
    url = config["master_data_url"]["url"]


    iudx_username = config["iudx_credentials"]["username"]
    iudx_password = config["iudx_credentials"]["password"]

    md = master_data(url, queue, iudx_username, iudx_password)
    serverconfigure = RabbitMqServerConfigure( username, password, host, port, vhost, queue)
    server = rabbitmqServer(server=serverconfigure)
    server.startserver(md.process_request)


