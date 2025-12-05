from threading import Thread
from etl_worker import ETLWorker
from metrics_api import MetricsAPI
#from metrics import CollectMetrics
#from 'MongoDB Connection'.mongodb_connection import MongoInsert

etl_worker = ETLWorker()
api_server = MetricsAPI(r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\logs\metrics_log.json")


Thread(target=etl_worker.run, daemon=True).start()
api_server.run()
#CollectMetrics.run()
#MongoInsert.run()