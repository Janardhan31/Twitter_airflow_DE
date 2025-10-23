import tweepy
import pandas as pd
from datetime import datetime
import s3fs

def run_twitter_etl():
    bearer_token = "AAAAAAAAAAAAAAAAAAAAABjj4wEAAAAAWbj6hzhDOCC4xa6jZYwaLJLSFlY=ZBMwuVkFCBdakWCYvzo2FKLEByCpbOcPUJEuYMomxEJGTY8ej6"

    client = tweepy.Client(bearer_token=bearer_token)

    user = client.get_user(username="elonmusk")
    user_id = user.data.id

    tweets = client.get_users_tweets(
        id=user_id,
        max_results=100,  
        tweet_fields=["created_at", "public_metrics", "text"]
    )

    tweet_data = []
    if tweets.data:  
        for tweet in tweets.data:
            tweet_data.append({
                "user": "elonmusk",
                "text": tweet.text,
                "created_at": tweet.created_at,
                "retweet_count": tweet.public_metrics["retweet_count"],
                "like_count": tweet.public_metrics["like_count"]
            })

    df = pd.DataFrame(tweet_data)

    df.to_csv("refined_tweets.csv", index=False)
