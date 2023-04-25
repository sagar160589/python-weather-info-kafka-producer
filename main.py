import time
import os
from producer.producer import Producer
import requests, random

city_list = ['PUNE','KOLHAPUR','SATARA','NASHIK','MUMBAI','NAGPUR','KARAD']
producer = Producer()
WEATHER_API = os.environ.get('WEATHER_API')
weather_flag = True

def produce_message():
    weather_response = requests.get(f"{WEATHER_API}?access_key={os.environ.get('API_KEY')}"
                                    f"&query={random.choice(city_list)}")
    weather_data = weather_response.json()
    print(weather_data)
    if 'request' in weather_data:
        #Send weather data into Kafka broker
        producer.produce_weather_data(weather_data)
        return "Message Sent"
    else:
        produce_message()

while weather_flag:
    for i in range(2):
        produce_message()
        time.sleep(10)
    weather_flag = False


