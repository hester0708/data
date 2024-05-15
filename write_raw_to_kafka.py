import os
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
import time

folder_path = '/home/lighthouse/data/extracted'
topics = []
dfs = []

for file in os.listdir(folder_path):
    topics.append(file[:-4])
    df = pd.read_csv(os.path.join(folder_path, file), engine='python')
    dfs.append(df)
    print(file)

current_time = datetime.now().strftime('%H:%M:%S')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
start_time = time.time()

while time.time() - start_time <= 600:
    new_current_time = datetime.now().strftime('%H:%M:%S')
    while(new_current_time == current_time):
        new_current_time = datetime.now().strftime('%H:%M:%S')
    old_current_time = current_time
    current_time = new_current_time
    print(current_time)
    for index, df in enumerate(dfs):
        for _, row in df.iterrows():
            if row['simulation_time'] is not None and row['simulation_time'] > old_current_time:
                if row['simulation_time'] <= current_time:
                    future = producer.send(topics[index], value = row.to_json().encode('utf-8'))
                    result = future.get(timeout= 10)
                else:
                    break
                