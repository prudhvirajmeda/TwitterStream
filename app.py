import tweepy
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
from confluent_kafka import Producer, KafkaError

CONSUMER_KEY = '1dgcBu310Ts7hWAmXQrRgN7dw' # VALUE
CONSUMER_SECRET = 'TAq1NyB3orQo9XVk19thsgMI5GjSQhvpO5JnPOPpXD7PgSeE2T' # VALUE
ACCESS_TOKEN = '138816577-5itSgNqZ4yDyxOqST1fEJJMBcKQJB93oBtZe0cYj' # VALUE
ACCESS_TOKEN_SECRET = 'tAxATECQhdHTvp6y66oQlefd2Rk0l2mDNfoyRYFoxuZae' # VALUE



'''
Kafka setup
'''

# create Kafka config object
# https://kafka.apache.org/documentation/#producerconfigs

# instantiate producer


class StdOutListener(StreamListener):


    def on_data(self, data):
    	p.produce('test', data.encode('utf-8'))

        

    def on_error(self, status):
        print status
       

if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    p = Producer({'bootstrap.servers':'localhost:9092'})
    l = StdOutListener()
    auth = OAuthHandler(CONSUMER_KEY,CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN,ACCESS_TOKEN_SECRET)
    stream = Stream(auth, l)
    stream.filter(track=['apple','samsung'])



