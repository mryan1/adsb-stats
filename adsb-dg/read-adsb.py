import pyModeS as pms
from pyModeS.extra.tcpclient import TcpClient
import os
import redis
import urllib.request
import csv
import time
import datetime
from opensearchpy import OpenSearch
from datetime import datetime
import json

ADSBHOST = os.environ['ADSBHOST']
BEASTPORT = os.environ['BEASTPORT']
OSHOST = os.environ['OSHOST']
OSPORT = os.environ['OSPORT']

dburl = 'https://opensky-network.org/datasets/metadata/aircraftDatabase.csv'
dbFileName = 'aircraftData.csv'
dbtype = os
esIndex = 'aircraft3'

class redisADSBClient(TcpClient):
    def __init__(self, host, port, rawtype, redisClient):
        super(redisADSBClient, self).__init__(host, port, rawtype)
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
                #TO-DO: do fuzzy match on existing items and only insert in overall "models:" if it doesn't match an existing item
                # https://github.com/seatgeek/thefuzz
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
                    self.updateRedisPlanes(frozenset(new))

class osADSBClient(TcpClient):
    def __init__(self, host, port, rawtype, searchClient):
        super(osADSBClient, self).__init__(host, port, rawtype)
        self.currentICAO = {}
        self.oldICAO = {}
        self.sc = searchClient

    def updateCurrentICAO(self, icao, ts):
        self.currentICAO[icao] = ts
        #prune icaos that aren't around anymore
        self.currentICAO = {k:v for (k,v) in self.currentICAO.items() if v > ts-300}

    def updateSeenCounter(self, icao):
        update = ''' {
                    "scripted_upsert": true,
                    "script": {
                        "source": "ctx._source.counter += params.count",
                        "lang": "painless",
                        "params": {
                        "count": 1
                        }
                    },
                    "upsert": {
                        "counter": 1
                    }            
                } '''
        try:
            response = self.sc.update(index='aircraft', id=str.lower(icao), body=update)
        except Exception as e:
            print(e)

    def updateSeenDateTimes(self,icao):
        dt = str(datetime.now())
        update = '''
            {{
            "scripted_upsert": true,
            "script": {{
                "source": "ctx._source.datesseen.add(params.dt)",
                "lang": "painless",
                "params": {{
                    "dt": "{seentime}"
                    }}
                }}
            }}
        '''
        try:
            response = self.sc.update(index='aircraft', id=str.lower(icao), body=update.format(seentime=dt))
        except Exception as e:
            print(e.info)

    def updateOsPlanes(self, newICAO):
        date = datetime.today().strftime('%Y-%m-%d')
        for i in newICAO:
            self.updateSeenCounter(i)
            self.updateSeenDateTimes(i)

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
                    self.updateOsPlanes(frozenset(new))

def updateOsDB(searchClient):
    print("updating OpenSearch base data")
    #TODO: add datesseen and counter fields
    #TODO: skip csv header
    #urllib.request.urlretrieve(dburl, dbFileName)
    with open(dbFileName, newline='') as csvfile:
        reader = csv.reader(csvfile)
        ndjson = []
        count = 0
        for row in reader:
            ndjson.append({"index": {"_id" :row[0]}})
            ac = {"ICAO":row[0], "registration":row[1], "manufacturername":row[3], "model":row[4], "serialnumber":row[6], "owner":row[13], "built": row[18] }
            ndjson.append(ac)
            count = count+1
            if count >= 100:
                response = searchClient.bulk(
                            body = ('\n'.join(map(json.dumps, ndjson))),
                            index = 'aircraft'
                        )
                print('\n Performing bulk import:')
                print(response)
                ndjson.clear()
                count = 0
                time.sleep(1.5)
    with open('updateTime', 'w') as updateTime:
        updateTime.write(str(time.time())) 
    
searchClient = OpenSearch(
        hosts = [{'host': OSHOST, 'port': OSPORT}],
        http_compress = True, # enables gzip compression for request bodies
        use_ssl = True,
        verify_certs = True,
        ssl_assert_hostname = False,
        ssl_show_warn = False,
    )

# populate db with aircraft data from https://opensky-network.org/datasets/metadata/
with open('updateTime', 'r') as updateTime:
    try:
        if (time.time() - float(updateTime.read())) > 2628000:
            print("Updating DB...")
            updateOsDB(searchClient)
    except Exception as e: 
        print(e)
        updateOsDB(searchClient)
    client = osADSBClient(ADSBHOST, BEASTPORT, 'beast', searchClient)
    client.run()