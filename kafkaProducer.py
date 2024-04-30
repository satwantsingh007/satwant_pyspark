from kafka import KafkaProducer
import os
from time import sleep

#this code take log data with the help of nifi and stores in local path

producer = KafkaProducer(bootstrap_servers='localhost:9092')
path = 'C:\\bigdata\\TESTING\\'
file_list = os.listdir(path)
for file in file_list:
    with open(path + str(file), errors="ignore", mode="r") as f:
        line = f.read()
        print(line)
        producer.send('apr28', line.encode('utf-8'))
        sleep(5)
        #Kafka every second 1 million messages sending so all ur messages send within 1 sec .. so it's not visible
        #if u mention sleep(5) every log wait 5 sec next sent .. that's why u r using sleep(5)

