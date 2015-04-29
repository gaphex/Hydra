__author__ = 'denisantyukhov'
import requests
import tweepy
from meta import *
import ast


class Oracle():
    def __init__(self):
        self.key = 'AIzaSyCtaVbVYJrHPdbkj_gpxQWktZ-_5sJRyVk'
        self.gmaps = 'https://maps.googleapis.com/maps/api/geocode/json'


    def requestCoordinates(self, location):
        response = requests.get(self.gmaps, params={'address' : location, 'key' : self.key})
        responseJSON = response.json()

        if responseJSON['status'] == 'OK':
            result = responseJSON['results'][0]
            return {'lat':result['geometry']['location']['lat'],
                    'lng':result['geometry']['location']['lng'],
                    'formatted_address':result['formatted_address']}
        else:
            return None

    def findInRaw(self, request, raw):
        u = raw.find(request)

        if u != -1:
            try:
                if request == 'bounding_box':
                    r1 = raw[u:u+200].split('{')[1].split('}')[0]
                    u = r1.find('[[[')
                    box = ast.literal_eval(r1[u+1:u+105])
                    return box
                else:
                    return raw[u:u+30].split(' ')[1].split("'")[1]
            except Exception as e:
                return 0
        else:
            return 0

    def whereIsIt(self, tweet):
        if tweet.geodata:
            geodata = tweet.geodata['coordinates']
            tweet.trueLocation = {'lat':geodata[0], 'lon':geodata[1]}
        elif len(tweet.location):
            apiResponse = self.requestCoordinates(tweet.location)
            tweet.trueLocation['lat'] = apiResponse['lat']
            tweet.trueLocation['lon'] = apiResponse['lng']
            tweet.trueLocation['text'] = apiResponse['formatted_address']
        else:
            tweet.trueLocation = None
        print tweet.trueLocation

def initAPIKeys(nP):
    auths = []

    for i in range(nP):
        auth = tweepy.OAuthHandler(CONSUMER_KEY[i], CONSUMER_SECRET[i])
        auth.set_access_token(OAUTH_TOKEN[i], OAUTH_TOKEN_SECRET[i])
        auths.append(auth)
    return auths




