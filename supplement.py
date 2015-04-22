__author__ = 'denisantyukhov'
import requests
import tweepy
from meta import *

def initAPIKeys():
    auths = []

    for i in range(5):
        auth = tweepy.OAuthHandler(CONSUMER_KEY[i], CONSUMER_SECRET[i])
        auth.set_access_token(OAUTH_TOKEN[i], OAUTH_TOKEN_SECRET[i])
        auths.append(auth)
    return auths

def getLocationCoordinates(location):
    response = requests.get('https://maps.googleapis.com/maps/api/geocode/json',
                            params={'address' : location, 'key' : 'AIzaSyCtaVbVYJrHPdbkj_gpxQWktZ-_5sJRyVk'})
    responseJSON = response.json()

    if responseJSON['status'] == 'OK':
        result = responseJSON['results'][0]
        return {'lat':result['geometry']['location']['lat'],
                'lng':result['geometry']['location']['lng'],
                'formatted_address':result['formatted_address']}
    else:
        return None

def processGeodata(tweet):
        if tweet.geodata:
            geodata = tweet.geodata['coordinates']
            tweet.trueLocation = {'lat':geodata[0], 'lon':geodata[1]}
        elif len(tweet.location):
            apiResponse = getLocationCoordinates(tweet.location)
            tweet.trueLocation['lat'] = apiResponse['lat']
            tweet.trueLocation['lon'] = apiResponse['lng']
            tweet.trueLocation['text'] = apiResponse['formatted_address']
        else:
            tweet.trueLocation = None
        print tweet.trueLocation