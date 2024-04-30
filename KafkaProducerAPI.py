from kafka import KafkaConsumer
import os

path = "C:\\bigdata\\TESTING\\"
file_path = os.listdir(path)

for file in file_path:
    with open(path+str(file), errors="ignore", mode="r") as f:
        lines = f.read()
        print(lines)



