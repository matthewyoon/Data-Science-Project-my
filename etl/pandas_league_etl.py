import pandas as pd
import pandas as pd
import datetime as dt
from pandas import json_normalize, Series
from riotwatcher import LolWatcher, ApiError
from datetime import timedelta
from sqlalchemy.sql.sqltypes import Float
from sqlalchemy.types import Integer, Text, DateTime
from dotenv import load_dotenv

load_dotenv()

# Goal of the ETL is to pull the statistics of matches and figure the overall kill/death ratio of that user over a period of averaged games.
# The end-goal of the analysis of the database is to figure a ratio over a large sum of games to determine an overall average.
class Pandas_ETL():

    # Initialize self variables that can be used within the class. ie. API-Key, riotwatcher wrapper, region and the season
    def __init__(self):
        self.API_KEY='RGAPI-beefeee6-bcef-480e-bcdc-12d3a372ece0'
        self.lol_watcher = LolWatcher(self.API_KEY)
        self.my_region = 'na1'
        self.season = 13

    # Method to call the api and find specific data regarding the requested username
    def get_summonerData(self, username):

        summoner = self.lol_watcher.summoner.by_name(self.my_region, username)
        summoner_df = pd.DataFrame([summoner])
        return summoner_df

    def get_summonerRanks(self, name_df):
        rank = pd.DataFrame()
        for i in range(len(name_df)):
            temp = pd.DataFrame(self.lol_watcher.league.by_summoner('na1', name_df['player.summonerId'][i]))
            rank = rank.append(temp)
        ranks = rank[['tier','rank']].copy()
        ranks.reset_index(drop=True,inplace=True)
        return ranks

    # Method to get the specific match ids of the requested username (these match id's are required to access the match portion of the API that houses users' statistics)
    def get_match_id(self, summoner_df, num_matches):

        account_id = summoner_df['accountId'][0]
        match = self.lol_watcher.match.matchlist_by_account(region=self.my_region, encrypted_account_id=account_id, end_index=num_matches, season=self.season)
        match_df = pd.DataFrame(match['matches'])
        match_id = match_df['gameId']
        return match_id

    # Method to grab the statistics of specific matches. 
    def get_stats(self, match_id):
        match_info = [] # initializing a list to hold the information we get from the API
        stats = []
        stats_df = pd.DataFrame() # Initializing an empty dataframe
        
        for i in range(len(match_id)):
            data = self.lol_watcher.match.by_id(region=self.my_region, match_id = match_id[i])
            data_df = json_normalize(data) # this dataframe will hold the api data - json_normalize to convert the raw json file to a more readable format
            match_info = data_df['participants'].values.tolist() 
            match_df = pd.concat([pd.DataFrame(t) for t in match_info], axis=1)
            stats = match_df['stats'].values.tolist() # the stats column is held within a dictionary. Want to append that to our empty list
            temp_df = pd.concat([pd.DataFrame(s,index=[0]) for s in stats]) # Due to the nature of the "to_list" method, will need to flatten our np.array because it looks like  [ [ {} ] ]
            stats_df = stats_df.append(temp_df) # our final statistics dataframe with the data we pulled from the api appended to it.
        stats_df.reset_index(drop=True,inplace=True)    
        return stats_df

    # This method is to obtain a different dictionary housed within the match portion of the API
    # The statistics are indexed based on match participant id's but those id's aren't attached to a name within the statistics dictionary
    def get_summonerName(self, match_id):
        name_df = pd.DataFrame()
        
        for i in range(len(match_id)):
            data = self.lol_watcher.match.by_id(region = self.my_region, match_id = match_id[i])
            data_df = json_normalize(data['participantIdentities'])
            name_df = name_df.append(data_df)
            
        name_df = name_df.drop(columns=['participantId','player.currentPlatformId','player.currentAccountId','player.matchHistoryUri','player.profileIcon','player.platformId']) # Dropping duplicate columns    
        name_df.reset_index(drop=True,inplace=True) # Will need to reset the index after pulling the names of the match participants
        return name_df

    # This extract method will call upon all of the other methods within the clas to return a final "extracted" dataframe
    # The only parameters it will take in from the user will be a specific username and the number of matches they want to calculate(obviously the more matches, the more reflective the kill/death ratio will be of their abilities)
    def extract(self, username, num_matches):
        summoner_df = self.get_summonerData(username)
        match_id = self.get_match_id(summoner_df, num_matches)
        stats_df = self.get_stats(match_id)
        name_df = self.get_summonerName(match_id)
        # match_ranks_df = self.get_summonerRanks(name_df)
        # name_df = name_df.join(match_ranks_df)
        temp_df = name_df.join(stats_df)
        # Will want to specify which columns I want 
        extracted_df = temp_df[['player.summonerName','player.summonerId','kills','deaths','assists','totalDamageDealtToChampions','visionScore','totalMinionsKilled']].copy()

        return extracted_df

    # This method is to calculate the actual kill/death ratio
    # Could have used a lambda function, but this method makes the purpose a little more clear imo.
    def kda(self, row):
        if row['deaths'] == 0:
            return None
        else:
            return round(row['kills'] + row['assists'] / row['deaths'], 2)
    
    # This transformation will have the data from the number of matches the user inputs and add that to the database as a dataframe.
    # In this example, user "Doublelift"'s most recent 5 matches will be taken into account.
    def transform(self, username, num_matches):
        extracted_df = self.extract(username, num_matches)
        extracted_df = extracted_df.where(pd.notnull(extracted_df), None)
        # print(extracted_df)
        extracted_df = extracted_df.groupby(['player.summonerName'], as_index=False)[['kills','deaths','assists','totalDamageDealtToChampions','visionScore','totalMinionsKilled']].mean().round(2)
        extracted_df_username = extracted_df.loc[extracted_df['player.summonerName']==username].copy()
        print(extracted_df_username)
        

        # Check if df is empty
        if extracted_df.empty:
            print('No Data. Please check your API key or username input')
            return False
        
        # Primary Key Check
        
        if pd.Index(extracted_df).is_unique:
            pass
        else:
            raise Exception(f'[Transform Error]: Primary Key Check is not valid')
        
        if extracted_df.isnull().values.any():
            raise Exception('No real Values Found')

        # Adding Transformation Column - Pop range Column
        extracted_df_username['kill/death/assist ratio'] = extracted_df_username.apply(self.kda, axis=1)
        extracted_df_username['numMatchesInputted'] = num_matches

        print(extracted_df_username.sort_values(['kill/death/assist ratio'], ascending=False))
        return extracted_df_username

    def load(self, username, num_matches): # similar to the other methods - will take in 2 params
        df = self.transform(username, num_matches)
        connection = 'postgresql://postgres:twdiplapo22@127.0.0.1:5432/recent_matches_kda_data'

        df.to_sql('recent_artist_popularity', index=False, con = connection, if_exists = 'append',
        schema = 'public', chunksize = 500, dtype={
                                                    'player.summonerName': Text,
                                                    'kills': Float,
                                                    'deaths': Float,
                                                    'assists': Float,
                                                    'totalDamageDealtToChampions': Float,
                                                    'visionScore': Float,
                                                    'totalMinionsKilled': Float,
                                                    'numMatchesInputted': Integer
                                                })

        return df

etl=Pandas_ETL()
final_df = etl.load('rickyyytan',5)

#argparse

