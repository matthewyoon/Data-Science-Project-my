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
from pandas import json_normalize
from riotwatcher import LolWatcher, ApiError
from datetime import timedelta
from sqlalchemy.types import Integer, Text, DateTime
from dotenv import load_dotenv

load_dotenv()


class Pandas_ETL():
    def __init__(self):
        self.API_KEY='RGAPI-beefeee6-bcef-480e-bcdc-12d3a372ece0'
        self.lol_watcher = LolWatcher(self.API_KEY)
        self.my_region = 'na1'
        self.season = 13


    def get_summonerData(self, username):

        summoner = self.lol_watcher.summoner.by_name(self.my_region, username)
        summoner_df = pd.DataFrame([summoner])
        return summoner_df

    def get_match_id(self, summoner_df, num_matches):

        account_id = summoner_df['accountId'][0]
        match = self.lol_watcher.match.matchlist_by_account(region=self.my_region, encrypted_account_id=account_id, end_index=num_matches, season=self.season)
        match_df = pd.DataFrame(match['matches'])
        match_id = match_df['gameId']
        return match_id

    def get_stats(self, match_id):
        match_info = []
        stats = []
        stats_df = pd.DataFrame()
        
        for i in range(len(match_id)):
            data = self.lol_watcher.match.by_id(region=self.my_region, match_id = match_id[i])
            data_df = json_normalize(data)
            match_info = data_df['participants'].values.tolist() 
            match_df = pd.concat([pd.DataFrame(t) for t in match_info], axis=1)
            stats = match_df['stats'].values.tolist()
            temp_df = pd.concat([pd.DataFrame(s,index=[0]) for s in stats])
            stats_df = stats_df.append(temp_df)
        stats_df.reset_index(drop=True,inplace=True)    
        return stats_df

    def get_summonerName(self, match_id):
        name_df = pd.DataFrame()
        
        for i in range(len(match_id)):
            data = self.lol_watcher.match.by_id(region = self.my_region, match_id = match_id[i])
            data_df = json_normalize(data['participantIdentities'])
            name_df = name_df.append(data_df)
            
        name_df = name_df.drop(columns=['participantId','player.currentPlatformId','player.currentAccountId','player.matchHistoryUri','player.profileIcon','player.platformId'])    
        name_df.reset_index(drop=True,inplace=True)
        return name_df

    def extract(self, username, num_matches):
        summoner_df = self.get_summonerData(username)
        match_id = self.get_match_id(summoner_df, num_matches)
        stats_df = self.get_stats(match_id)
        name_df = self.get_summonerName(match_id)
        temp_df = name_df.join(stats_df)
        extracted_df = temp_df[['player.accountId','player.summonerName','player.summonerId','participantId','win','kills','deaths','assists','largestKillingSpree','largestMultiKill','totalDamageDealtToChampions','magicDamageDealtToChampions','physicalDamageDealtToChampions','visionScore','totalMinionsKilled']].copy()
        

    def kda(self, extracted_df):
        extracted_df['kda'] = extracted_df['kill/death/assist ratio']



    def transform(self, username, num_matches):
        extracted_df = self.extract(username, num_matches)
        extracted_df = extracted_df.groupby(['player.summonerName'])[['kills','deaths','assists']].mean()

        # Check if df is empty
        if extracted_df.empty:
            print('No Data. Please check your API key or username input')
            return False
        
        # Primary Key Check
        if pd.Series(df['player.summonerName']).is_unique:
            pass
        else:
            raise Exception(f'[Transform Error]: Primary Key Check is not valid')
        
        if extracted_df.isnull().values.any():
            raise Exception('No real Values Found')

        # Adding Transformation Column - Pop range Column
        extracted_df['kda'] = extracted_df['kill/death/assist ratio'].apply(self.pop_check)

        print(extracted_df)
        return extracted_df

etl=Pandas_ETL()
final_df = etl.transform('Doublelift',2)





# etl.get_summonerData(username='Doublelift')


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


etl = Pandas_ETL()

# etl.load()