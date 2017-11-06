# Test twitter agg

Testing Spark with Bahir

Connects to the twitter sample endpoint and outputs the following aggregated data:
1. Total tweets
2. Tweets per hour/minute/second
3. Percentage of tweets that contain urls
4. Percentage of tweets that contain pictures
5. Percentage of tweets that contain Emojis
6. Top 10 Emojis
7. Top 10 Domains linked
8. Top 10 Hashtags

### To run:
1. Compile the jar using sbt package
2. Install Spark 2.2.0
3. Execute Spark submit :
>spark-submit --master local[4] --packages org.apache.bahir:spark-streaming-twitter_2.11:2.2.0 --class com.jrancier.twitter.TwitterAgg  jarLocation consumerKey consumerSecret accessToken accessTokenSecret

Make sure to fill in the required values.
