import json

############################################################################################
###############################  Code to Extract Tweets   ##################################

# import tweepy
# import pandas as pd
# import s3fs
# import configparser
# from datetime import datetime


# def apiTwitter():
#     # read credentials
#     config = configparser.ConfigParser()
#     config.read('credentialsTwitter.ini')

#     api_key = config['twitter']['api_key']
#     api_key_secret = config['twitter']['api_key_secret']

#     access_token = config['twitter']['access_token']
#     access_token_secret = config['twitter']['access_token_secret']

#     # authentication
#     auth = tweepy.OAuthHandler(api_key,api_key_secret)
#     auth.set_access_token(access_token,access_token_secret)

#     api=tweepy.API(auth)

#     return api


# api=apiTwitter()
# tweets = api.user_timeline(
#     screen_name='@elonmusk',
# #     # 200 is the maximum allowed count
#     count=200,
#     include_rts=False,
# #     # Necessary to keep full_text
# #     # Otherwise only the first 140 words extracted
#     tweet_mode='extended'
# )
# tweet=tweets[0]
# #print(tweet._json)
# print(f'Tweet text: {tweet.full_text}')

############################################################################################

import pandas as pd


def run_twitter_etl():

    # Define the path to the JSON file
    json_file_path = "data.json"

    tweet_list = []

    # Read the JSON file
    with open(json_file_path, "r") as file:
        
        tweet_data = json.load(file)
        for tweet_obj in tweet_data:
            
            refined_tweet = {
                "username": tweet_obj['username'],
                "tweet": tweet_obj['tweet'],
                "retweet_count": tweet_obj['retweet_count'],
                "favorite_count": tweet_obj['favorite_count'],
                "followers_count": tweet_obj['followers_count'],
                "lang": tweet_obj['lang']
            }
            
            tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv("s3://kk-airflow-twitter-bucket/twitter_data.csv")

        