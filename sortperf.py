import os
import time
from borneo import (Regions, NoSQLHandle, NoSQLHandleConfig, PutRequest,
                    AuthorizationProvider, QueryRequest, TableRequest,
                    GetRequest)
from borneo.kv import StoreAccessTokenProvider

def get_connection():
    endpoint = 'localhost:80'
    print('Connecting to Oracle NoSQL Cloud Service at ' + endpoint)
    provider = StoreAccessTokenProvider() #CloudsimProvider()
    config = NoSQLHandleConfig(endpoint, provider).set_logger(None)
    return NoSQLHandle(config)

def fetch_data(handle):
    print('Fetching data')
    statement ="SELECT * FROM moat_integration_services_jobs WHERE tool = 'iqt' AND active = False ORDER BY time_created DESC"

    #statement ="SELECT * FROM moat_integration_services_jobs WHERE tool = 'iqt' AND active = False ORDER BY time_created DESC LIMIT 15 OFFSET 0"
    # statement ="SELECT count(1) FROM moat_integration_services_jobs WHERE tool = 'iqt' AND active = False"

    request = QueryRequest().set_statement(statement)
    print('Query results for: ' + statement)
    t1 = time.perf_counter()
    numres = 0
    lasttime = None
    while True:
      result = handle.query(request)
      res = result.get_results()
      for r in res:
        t = r['time_created']
        if lasttime is not None and not t <= lasttime:
          print('FAIL: not <= ' + lasttime + ', ' + t)
        lasttime = t
      numres += len(res)
      if request.is_done():
         break
    t2 = time.perf_counter()
    print(f"Time: {t2 - t1:0.4f} seconds")
    print('Num results: ' + str(numres))


def main():
    print("Inside main")
    handle = get_connection()
    fetch_data(handle)
    os._exit(0)


class CloudsimProvider(AuthorizationProvider):

    def __init__(self):
        super(CloudsimProvider, self).__init__()

    def close(self):
        pass

    def get_authorization_string(self, request=None):
        return 'Bearer sortperf'

if __name__ == "__main__":
    main()
