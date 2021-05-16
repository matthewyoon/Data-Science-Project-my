import pandas as pd
import requests
import datetime
import base64
import os
import pandas as pd
import re
import datetime as dt
import sys
import argparse
from sqlalchemy.types import Integer, Text, DateTime
from riotwatcher import LolWatcher, ApiError
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()


class Pandas_ETL():
    


    def get_summoner_data(self, my_region, username, watcher):

        summoner = watcher.summoner.by_name(my_region, username)
        summoner_df = pd.DataFrame([summoner])
        return summoner_df

    

# SPOTIFY ETL EXAMPLE
# class Pandas_ETL():
#     CLIENT_ID = os.environ.get('SP_CLIENT_ID')
#     CLIENT_SEC = os.environ.get('SP_CLIENT_SECRET')

#     # Data Getting method
#     def get_data(self):
#         scope = 'user-library-read user-read-recently-played'

#         today = datetime.datetime.now()
#         past_90 = today - datetime.timedelta(days = 90)
#         past_90_unix_timestamp = int(past_90.timestamp()) * 1000

#         sp = spotipy.Spotify(   auth_manager=SpotifyOAuth(client_id=self.CLIENT_ID,
#                                 client_secret=self.CLIENT_SEC,
#                                 redirect_uri='http://localhost:3000/callback',
#                                 scope=scope)
#                             )

#         return sp.current_user_recently_played(limit=40)
    
#     # Extract Method
#     def extract(self):
#         data = self.get_data()

#         # Values inside of Pandas Dataframe
#         song_names = []
#         artist_names = []
#         played_at = []
#         popularity = []

#         for song in data['items']:
#             song_names.append(song['track']['name'])
#             artist_names.append(song['track']['album']['artists'][0]['name'])
#             played_at.append(song['played_at'])
#             popularity.append(song['track']['popularity'])

#         song_dict = {
#                         'song_names': song_names,
#                         'artist_names': artist_names,
#                         'played_at': played_at,
#                         'popularity': popularity
#                     }

#         song_df = pd.DataFrame(song_dict,columns = ['song_names','artist_names','played_at','popularity'])

#         print(song_df)

#         return song_df

#     def pop_check(self, pop_number):
#         if pop_number > 50:
#             return 'High'
#         else:
#             return 'Low'
    
#     # Transform Method
#     def transform(self):
#         df = self.extract()

#         # Check if df is empty
#         if df.empty:
#             print('No Songs Downloaded. Finishing Execution')
#             return False
        
#         # Primary Key Check
#         if pd.Series(df['played_at']).is_unique:
#             pass
#         else:
#             raise Exception(f'[Transform Error]: Primary Key Check is not valid')
        
#         if df.isnull().values.any():
#             raise Exception('No real Values Found')

#         # Adding Transformation Column - Pop range Column
#         df['pop_range'] = df['popularity'].apply(self.pop_check)

#         print(df)
#         return df

#     def load(self):
#         df = self.transform()
#         connection = 'postgresql://postgres:twdiplapo22@127.0.0.1:5432/90_day_song_data'

#         df.to_sql('recent_artist_popularity', index=False, con = connection, if_exists = 'append',
#         schema = 'public', chunksize = 500, dtype={
#                                                     'song_name': Text,
#                                                     'artist_names': Text,
#                                                     'played_at': DateTime,
#                                                     'popularity': Integer,
#                                                     'pop_range': Text
#                                                 })

#         return df


# etl = Pandas_ETL()

# etl.load()