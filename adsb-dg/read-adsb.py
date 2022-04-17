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
esIndex = 'aircraft4'

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
        update = ''' {{
                    "scripted_upsert": true,
                    "script": {{
                        "source": "ctx._source.counter += params.count",
                        "lang": "painless",
                        "params": {{
                        "count": 1
                        }}
                    }},
                    "upsert": {{
                        "ICAO": "{icao}",
                        "counter": 1
                    }}            
                }} '''
        print(update.format(icao=str.lower(icao)))
        try:
            response = self.sc.update(index=esIndex, id=str.lower(icao), body=update.format(icao=str.lower(icao)))
        except Exception as e:
            print(e.info)
#                   "datesseen": "[{seentime}]"
    def updateSeenDateTimes(self,icao):
        dt = str(datetime.utcnow().strftime("%Y-%m-%d"'T'"%H:%M:%S"))
        updateSeenDateTime = '''
            {{
            "scripted_upsert": true,
                "script": {{
                    "source": "if(ctx._source.datesseen == null){{ctx._source.datesseen = params.ds}} else {{ctx._source.datesseen.add(params.ds)}}",
                    "params": {{
                    "ds": "{seentime}"
                    }}
                }},
                "upsert": {{}}
            }}
        '''
        #print(updateSeenDateTime.format(seentime=dt, icao=str.lower(icao)))
        try:
            response = self.sc.update(index=esIndex, id=str.lower(icao), body=updateSeenDateTime.format(seentime=dt, icao=icao))
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
                            index = esIndex
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