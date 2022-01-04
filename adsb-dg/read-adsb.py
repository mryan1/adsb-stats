import pyModeS as pms
from pyModeS.extra.tcpclient import TcpClient
import os
import redis
import urllib.request
import csv
import time
from datetime import datetime

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

    def updateRedisPlanes(self, newICAO):
        date = datetime.today().strftime('%Y-%m-%d')
        for i in newICAO:
            self.rc.zincrby(("planes:" + date), 1, i)

            key = ("icao:" + i).lower()
            #get plane info
            m = self.rc.hget(key, "model")
            o = self.rc.hget(key,"owner")
            y = self.rc.hget(key, "built")
            ma = self.rc.hget(key, "manufacturername")

            if m:
                mn = ma + " " + m
                #per day stats
                self.rc.zincrby(("models:" + date), 1, mn)
                #overall stats
                self.rc.zincrby(("models"), 1, mn)
                #add set to track model -> icao
                self.rc.sadd((mn.lower().replace(" ", "_") + ":icao:" + date), i)
            if o:
                self.rc.zincrby(("owners:" + date), 1, o)
                self.rc.zincrby(("owners"), 1, o)
                self.rc.sadd((o.lower().replace(" ", "_") + ":icao:" + date), i)
            if y:
                self.rc.zincrby(("years"), 1, y)
                self.rc.zincrby(("years:" + date), 1, y)
                self.rc.sadd((y + ":icao:" + date), i)
                   
    def updateCurrentICAO(self, icao, ts):
        self.currentICAO[icao] = ts
        #prune icaos that aren't around anymore
        self.currentICAO = {k:v for (k,v) in self.currentICAO.items() if v > ts-300}

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
    with open(dbFileName, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            ac = {"registration":row[1], "manufacturername":row[3], "model":row[4], "serialnumber":row[6], "owner":row[13], "built": row[18]}
            r.hmset("icao:" + row[0], ac)

r = redis.Redis(host=REDISSERVER, port=REDISPORT, db=0, encoding="utf-8", decode_responses=True)

# populate reddis with aircraft data from https://opensky-network.org/datasets/metadata/
if (time.time() - float(r.get("dbUpdateTime"))) > 2628000:
    print("Updating DB...")
    updateDB()
r.set("dbUpdateTime", time.time())

client = ADSBClient(host=ADSBHOST, port=BEASTPORT, rawtype='beast', redisClient=r)
client.run()