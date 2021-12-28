import pyModeS as pms
from pyModeS.extra.tcpclient import TcpClient
import os
import redis
from cachetools import cached, TTLCache
import urllib.request
import csv
import time


ADSBHOST = os.environ['ADSBHOST']
BEASTPORT = os.environ['BEASTPORT']
REDISSERVER = os.environ['REDISSERVER']
REDISPORT = os.environ['REDISPORT']
dburl = 'https://opensky-network.org/datasets/metadata/aircraftDatabase.csv'
dbFileName = 'aircraftData.csv'

class ADSBClient(TcpClient):
    def __init__(self, host, port, rawtype, redisClient):
        super(ADSBClient, self).__init__(host, port, rawtype)
        self.rc = redisClient
        self.currentICAO = {}
        self.oldICAO = {}


    @cached(cache = TTLCache(maxsize = 30, ttl = 300))            
    def updateRedisPlanes(self, newICAO):
        for i in newICAO:
            self.rc.zincrby("planes", 1, i)

            key = ("icao:" + i).lower()
            #get plane model and incr
            m = self.rc.hget(key, "model")
            if m:
                self.rc.zincrby("models", 1, m)

            #get year built and incr 
            y = self.rc.hget(key, "built")
            if y:
                self.rc.zincrby("years", 1, y)


    def updateCurrentICAO(self, icao, ts):
        self.currentICAO[icao] = ts
        #prune icaos that aren't around anymore
        self.currentICAO = {k:v for (k,v) in self.currentICAO.items() if v > ts-120}

    def handle_messages(self, messages):
        for msg, ts in messages:
            if len(msg) != 28:  # wrong data length
                continue

            df = pms.df(msg)

            if df != 17:  # not ADSB
                continue

            if pms.crc(msg) !=0:  # CRC fail
                continue

            self.oldICAO = self.currentICAO.copy()
            icao = pms.adsb.icao(msg)
            self.updateCurrentICAO(icao, ts)

            if self.oldICAO != self.currentICAO:
                #update redis with any new ICAOS
                new = set(self.currentICAO) - set(self.oldICAO)
                if len(new) > 0:
                    print("New: ", new)
                    print("Current: ", self.currentICAO)
                    self.updateRedisPlanes(frozenset(new))

def updateDB():
    urllib.request.urlretrieve(dburl, dbFileName)
    print("Updating DB...")
    with open(dbFileName, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            ac = {"registration":row[1], "manufacturername":row[3], "model":row[4], "serialnumber":row[6], "owner":row[13], "built": row[18]}
            r.hmset("icao:" + row[0], ac)

r = redis.Redis(host=REDISSERVER, port=REDISPORT, db=0)

# populate reddis with aircraft data from https://opensky-netwo
# rk.org/datasets/metadata/
#TODO: add key with last time the db was updated, and only update if it's been more than a month since the last update
#urllib.request.urlretrieve(url, file_name)
if (time.time() - float(r.get("dbUpdateTime"))) > 2628000:
    updateDB()
    r.set("dbUpdateTime", time.time())

client = ADSBClient(host=ADSBHOST, port=BEASTPORT, rawtype='beast', redisClient=r)
client.run()