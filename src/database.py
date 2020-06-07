from datetime import datetime
import os
from src.consts import FROM_DATE_TIME, TILL_DATE_TIME, PM1, PM25, PM10, PRESSURE, HUMIDITY, \
    TEMPERATURE, SENSOR_ID, VALUES, NAME, VALUE, NO2, CO, WIND_BEARING, WIND_SPEED


def parse_date_string(date_string):
    return datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S.000Z')


def init_object(from_date_time, till_date_time, sensor_id):
    return (
        {
            SENSOR_ID: sensor_id,
            FROM_DATE_TIME: from_date_time,
            TILL_DATE_TIME: till_date_time,
            PM1: None,
            NO2: None,
            CO: None,
            PM25: None,
            PM10: None,
            PRESSURE: None,
            HUMIDITY: None,
            TEMPERATURE: None,
            WIND_BEARING: None,
            WIND_SPEED: None,
        }
    )


def execute(database, data):
    my_cursor = database.cursor()
    command = (
        "INSERT IGNORE INTO measurements (sensor_id, datetime_from, datetime_to, pm1, pm25, pm10, pressure, humidity, temperature, no2, co, wind_speed, wind_bearing) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        "ON DUPLICATE KEY UPDATE sensor_id=%s, datetime_from=%s, datetime_to=%s"
    )
    try:
        my_cursor.executemany(command, data)
        database.commit()
    except Exception as error:
        filename = 'logs.txt'
        if os.path.exists(filename):
            append_write = 'a'  # append if already exists
        else:
            append_write = 'w'

        file = open(filename, append_write)
        file.write("Exception thrown: {0}".format(error) + '\n')
        file.close()
        database.rollback()


def get_sql_data_record_for_insert(record, sensor_id):
    for_insert = get_object_for_database_insert(record, sensor_id)
    return (
        for_insert[SENSOR_ID],
        f'{for_insert[FROM_DATE_TIME]}',
        f'{for_insert[TILL_DATE_TIME]}',
        for_insert[PM1],
        for_insert[PM25],
        for_insert[PM10],
        for_insert[PRESSURE],
        for_insert[HUMIDITY],
        for_insert[TEMPERATURE],
        for_insert[NO2],
        for_insert[CO],
        for_insert[WIND_SPEED],
        for_insert[WIND_BEARING],
        for_insert[SENSOR_ID],
        f'{for_insert[FROM_DATE_TIME]}',
        f'{for_insert[TILL_DATE_TIME]}',
    )


def get_object_for_database_insert(record, sensor_id):
    from_date_time = parse_date_string(record[FROM_DATE_TIME])
    till_date_time = parse_date_string(record[TILL_DATE_TIME])
    object = init_object(sensor_id=sensor_id,
                         from_date_time=from_date_time,
                         till_date_time=till_date_time)
    for value in record[VALUES]:
        object[value[NAME]] = value[VALUE]
    return object
