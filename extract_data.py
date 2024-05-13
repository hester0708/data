import os
import pandas as pd
import datetime

raw_path = './raw'
unified_time_name = 'simulation_time'
extracted_path = './extracted'
os.mkdir(extracted_path)

def extract_data(raw_name, time_name, extracted_name, time_is_int = False, has_comment = False, comment_name = ''):
    df = pd.read_csv(os.path.join(raw_path, raw_name))
    if has_comment:
        df = df[df[comment_name].isna()]
    df = df[df[time_name].notna()]
    if time_is_int:
        df[time_name] = df[time_name].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%H:%M:%S'))
    else: 
        df[time_name] = df[time_name].apply(lambda x: x[11:19])
    df = df.rename(columns={time_name: unified_time_name})
    df = df.sort_values(by = unified_time_name)
    df.to_csv(os.path.join(extracted_path, extracted_name))
    return df

print('Facebook Data')
print(extract_data('test.csv', 'published_time', 'facebook.csv'))
print('YouTube Data')
print(extract_data('US_youtube_trending_data.csv', 'publishedAt', 'youtube.csv'))
print('Instagram Data')
print(extract_data('instagram_data.csv', 'created_at', 'instagram.csv', True))
print('TikTok Data')
print(extract_data('data.csv', 'datetime', 'tiktok.csv', False, True, 'comment_id'))
print('Twitter Data')
print(extract_data('twitter_dataset.csv', 'Timestamp', 'twitter.csv'))