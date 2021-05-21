# League of Legends Data Project using Riot API/Riotwatcher

This project was exploring the Riot API manually with for loops and api calls as well as using the riotwatcher wrapper.
The end goal was to be able to pull (avg) stats from a certain number of games for a specific username input.

## The ETL process involved extracting statistics data from the api

The current setup of etl involves inputting known usernames into the load method as well as a certain number of recent games from which the inputting user would like to receive data.

That data is then sent to (currently) a local postgresql database.

## Jupyter Notebooks
The jupyter notebooks currently within the repo show the specific methods used to create the dataframes needed. The statistics dictionaries in the api are several levels deep which is why I used the jupyter notebook to visualize the data that I wanted to see.

At first, the api calls were done manually by changing the id's in every url input, but a wrapper import called riotwatcher already enabled this. (although their wrappers to grab match data will be deprecated due to riot api's version changes).

# Results:

## The database will hold information regarding specific players within the north american region including their ranks. Comparisons can be made regarding the different tiers of players and what their average games and statistics might look like.

# Note:
The Riot api has rate limiters so pulling match data from 10+ recent matches might take a while as the functions have a built in sleep method to match the inherent rate limiter on the api.