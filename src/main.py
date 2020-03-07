import mysql.connector
import os
import sys
from datetime import datetime
sys.path.append('/home/wkd622/airly/')

from src.api import make_history_data_request
from src.consts import DATABASE_CONFIG
from src.database import get_sql_data_record_for_insert, execute

my_db = mysql.connector.connect(**DATABASE_CONFIG)

sensors_ids = [305, 304, 17]
data = {}
for sensor_id in sensors_ids:
    data[sensor_id] = make_history_data_request(sensor_id)

data_for_insert = []
for sensor_id in sensors_ids:
    for record in data[sensor_id]:
        data_for_insert.append(get_sql_data_record_for_insert(sensor_id=sensor_id, record=record))

execute(database=my_db, data=data_for_insert)

filename = 'logs.txt'
if os.path.exists(filename):
    append_write = 'a' # append if already exists
else:
    append_write = 'w'

file = open(filename, append_write)
file.write(str(datetime.now()) + '\n')
file.close()

my_db.close()
