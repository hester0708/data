import os
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer

folder_path = 'extracted'
topics = []
dfs = []
files = []
for file in os.listdir(folder_path):
    topics.append(file[:-4])
    df = pd.read_csv(os.path.join(folder_path, file))
    dfs.append(df)

current_time = datetime.now().strftime('%H:%M:%S')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

while True:
    new_current_time = datetime.now().strftime('%H:%M:%S')
    while(new_current_time == current_time):
        new_current_time = datetime.now().strftime('%H:%M:%S')
    old_current_time = current_time
    current_time = new_current_time
    for index, df in enumerate(dfs):
        for _, row in df.iterrows():
            if row['simulation_time'] > old_current_time:
                if row['simulation_time'] <= current_time:
                    future = producer.send(topics[index], value = row.to_json().encode('utf-8'))
                    result = future.get(timeout= 10)
                else: 
                    break

