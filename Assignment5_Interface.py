#!/usr/bin/python2.7
#
# Assignment3 Interface
# Name: Anandavaishnavi Ardhanari Shanmugam (1211256043)
#

from pymongo import MongoClient
import os
import sys
import json
import re
from math import radians, sin, cos, atan2, sqrt

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):

    # search the collection for businesses in cityToSearch regardless of its case
    # Reference: https://stackoverflow.com/questions/4976278/python-mongodb-regex-ignore-case
    businesses = collection.find({"city": re.compile(cityToSearch, re.IGNORECASE)})

    # opening file at saveLocation1 to write the businesses
    outputFileBusinessByCity = open(saveLocation1, "w")

    # every business found from the search query is written into the file
    for business in businesses:
        outputFileBusinessByCity.write(business['name'].upper().encode('utf-8') + "$" +
                            business['full_address'].replace("\n", ",").upper().encode('utf-8') + "$" +
                         business['city'].upper().encode('utf-8') + "$" +
                         business['state'].upper().encode('utf-8') +"\n")

    outputFileBusinessByCity.close();

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):

    # getting the latitude and longitude values from myLocation
    lat1 = float(myLocation[0])
    lon1 = float(myLocation[1])

    categoriesToSearchIgnoreCase = []
    # opening file at saveLocation1 to write the businesses
    outputFileBusinessByLoc = open(saveLocation2, "w")

    # creating a new list for the categoriesToSearch so as to perform a case insensitive search on the db
    # Reference: https://stackoverflow.com/questions/27363000/mongo-in-query-with-case-insensitivity
    for category in categoriesToSearch:
        item = re.compile(category, re.IGNORECASE)
        categoriesToSearchIgnoreCase.append(item)

    # finding all records in the collection which contain all of the categories given in categoriesToSearch
    # Reference - https://docs.mongodb.com/manual/reference/operator/query/all/
    businesses = collection.find({"categories":{"$all": categoriesToSearchIgnoreCase}})

    # getting all the businesses which are found within the maxDistance from myLocation
    for business in businesses:
        # getting the latitude and longitude values from business
        lat2 = business['latitude']
        lon2 = business['longitude']

        # calculating distance between myLocation and location of business
        distance = getDistance(lat2, lon2, lat1, lon1)
        if(distance <= maxDistance):

            # writing the businesses found within maxDistance in to the file
            outputFileBusinessByLoc.write(business['name'].replace("\n", ",").upper().encode('utf-8') + "\n")

    outputFileBusinessByLoc.close()

# function to get the distance between myLocation and business location
def getDistance(latitude2, longitude2, latitude1, longitude1):

    R = 3959
    lat1InRad = radians(latitude1)
    lat2InRad = radians(latitude2)
    latDiffInRad = radians(latitude2 - latitude1)
    lonDiffInRad = radians(longitude2 - longitude1)

    # a is the square of half the chord length between the points
    a = sin(latDiffInRad / 2.0) * sin(latDiffInRad/2.0) + cos(lat1InRad) * cos(lat2InRad) * sin(lonDiffInRad / 2.0) * sin(
        lonDiffInRad / 2.0)

    # c is the angular distance in radians
    c = 2.0 * atan2(sqrt(a), sqrt(1.0-a))

    # d is the distance between the given 2 points
    d = R * c

    return d

