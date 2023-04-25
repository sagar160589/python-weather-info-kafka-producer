from json import dumps
from kafka import KafkaProducer

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                      value_serializer=lambda x:dumps(x).encode('utf-8'))

    def produce_weather_data(self,weather_data):
        self.producer.send('weather_data',value=weather_data)