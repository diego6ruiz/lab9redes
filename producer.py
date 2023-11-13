import json
import random
import numpy as np
import time
from confluent_kafka import Producer

def generate_sensor_data():
    temperature = np.clip(np.random.normal(50, 10), 0, 100)
    relative_humidity = int(np.clip(np.random.normal(50, 15), 0, 100))
    wind_direction = random.choice(['N', 'NW', 'W', 'SW', 'S', 'SE', 'E', 'NE'])

    return {
        "temperatura": round(temperature, 2), 
        "humedad": relative_humidity, 
        "direccion_viento": wind_direction
    }

conf = { 
    #kafkaconfig
    'bootstrap.servers': 'lab9.alumchat.xyz:9092',
    'client.id': 'sensor-data-producer',
    'linger.ms': 1,
    'acks': 1,
    'retries': 5,
    'retry.backoff.ms': 1000
}

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to topic {} [{}]'.format(msg.topic(), msg.partition()))

def produce_sensor_data(interval):
    try:
        while True:
            data = generate_sensor_data()
            producer.produce('18761', json.dumps(data), callback=delivery_report)
            producer.poll(0)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Data transmission interrupted.")
    finally:
        print("Flushing queue...")
        producer.flush() #Manda el mensaje en queue
        print("Goodbye.")

producer = Producer(conf)
produce_sensor_data(15) #15s de intervalo