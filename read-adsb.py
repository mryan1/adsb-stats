import pyModeS as pms
from pyModeS.extra.tcpclient import TcpClient
import os
import redis
from cachetools import cached, TTLCache


ADSBHOST = os.environ['ADSBHOST']
BEASTPORT = os.environ['BEASTPORT']
REDISSERVER = os.environ['REDISSERVER']
REDISPORT = os.environ['REDISPORT']


# define your custom class by extending the TcpClient
#   - implement your handle_messages() methods
class ADSBClient(TcpClient):
    def __init__(self, host, port, rawtype):
        super(ADSBClient, self).__init__(host, port, rawtype)
        self.currentICAO = {}

    @cached(cache = TTLCache(maxsize = 1000, ttl = 1800))            
    def updateCurrentICAO(self, icao, ts):
        self.currentICAO[icao] = ts
        return self.currentICAO

    def handle_messages(self, messages):
        for msg, ts in messages:
            if len(msg) != 28:  # wrong data length
                continue

            df = pms.df(msg)

            if df != 17:  # not ADSB
                continue

            if pms.crc(msg) !=0:  # CRC fail
                continue

            icao = pms.adsb.icao(msg)
            tc = pms.adsb.typecode(msg)

            # TODO: write you magic code here
            print(ts, icao, tc, msg)
            #TODO: use cachetools to store ICAO values, if it's not in the cache then see if it's in reddis, and if not, add it
            #function can maintain a list of current ICAO values and return them.  TTL can be 30mins?
            print (self.updateCurrentICAO(icao, ts))


# populate reddis with aircraft data from https://opensky-network.org/datasets/metadata/

# run new client, change the host, port, and rawtype if needed
r = redis.Redis(host=REDISSERVER, port=REDISPORT, db=0)
r.set('foo', 'bar')

client = ADSBClient(host=ADSBHOST, port=BEASTPORT, rawtype='beast')
client.run()