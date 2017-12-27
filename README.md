# TwitterStream#Pipeline to stream tweets and write to local via Kafka topic and spark structured steam
The pipeline performs Trend Analytics from Tweets obtained from Twitter.
All the tweets which have the keyword eg: Apple , were captured using functions from tweepy libray as a stream
The stream was directed to kafka cluster for the topic 'test'
The stream of tweets for the topic 'test' were consuemd by spark streaming dataframes and written on local disk as 'Parquet' format





