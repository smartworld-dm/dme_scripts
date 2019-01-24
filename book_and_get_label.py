# Python 3.6.6

import sys, time
import os
import errno
import datetime
import uuid
import redis
import urllib, requests
import json
import pymysql, pymysql.cursors

production = True # Dev
# production = False # Local

if production:
    DB_HOST = 'fm-dev-database.cbx3p5w50u7o.us-west-2.rds.amazonaws.com'
    DB_USER = 'fmadmin'
    DB_PASS = 'Fmadmin1'
    DB_PORT = 3306
    DB_NAME = 'dme_db_dev'  # Dev
    # DB_NAME = 'dme_db_prod'  # Prod
else:
    DB_HOST = 'localhost'
    DB_USER = 'root'
    DB_PASS = 'root'
    DB_PORT = 3306
    DB_NAME = 'deliver_me'

redis_host = "localhost"
redis_port = 6379
redis_password = ""

def get_status_update_batch_size(mysqlcon):
    with mysqlcon.cursor() as cursor:
        sql = "SELECT `option_value` FROM `dme_options` WHERE `option_name`=%s"
        cursor.execute(sql, ('STATUS_UPDATE_BATCH_SIZE'))
        result = cursor.fetchone()
        return int(result['option_value'])

def get_available_bookings(mysqlcon, batch_size):
    with mysqlcon.cursor() as cursor:
        sql = "SELECT * FROM `dme_bookings` WHERE `b_status`=%s and `vx_freight_provider` IS NOT NULL ORDER BY `id` DESC LIMIT %s"
        cursor.execute(sql, ('Ready for booking', batch_size))
        result = cursor.fetchall()
        return result

def save_error(booking_id, error, mysqlcon):
    with mysqlcon.cursor() as cursor:
        sql = "UPDATE `dme_bookings` SET `b_error_Capture`=%s WHERE `id`=%s"
        cursor.execute(sql, (error, booking_id))
    mysqlcon.commit()

def book(booking, mysqlcon):
    if booking['vx_freight_provider'].lower() == 'allied':
        url = "http://ec2-35-161-196-46.us-west-2.compute.amazonaws.com/api/booking_allied/"
    elif booking['vx_freight_provider'].lower() == 'startrack':
        url = "http://ec2-35-161-196-46.us-west-2.compute.amazonaws.com/api/booking_st/"

    data = {'booking_id': booking['id']}
    headers = {'content-type': 'application/json'}
    response0 = requests.post(url, params={}, json=data, headers=headers)
    response0 = response0.content.decode('utf8').replace("'", '"')
    data0 = json.loads(response0)

    try:
        error = data0[0]['Error']
        print('#100, Book failed:', booking['id'], error)
        save_error(booking['id'], error, mysqlcon)
    except KeyError:
        try:
            success_booking_id = data0[0]['Created Booking ID']
            print('#101, Book success: ', success_booking_id)
            return True
        except KeyError:
            print('#102, Unknown Error: ', data0[0])

    # s0 = json.dumps(data0, indent=4, sort_keys=True)  # Just for visual
    # print(s0[0])

    return False

def get_label(booking, mysqlcon):
    if booking['vx_freight_provider'].lower() == 'allied':
        url = "http://ec2-35-161-196-46.us-west-2.compute.amazonaws.com/api/get_label_allied/"
    elif booking['vx_freight_provider'].lower() == 'startrack':
        url = "http://ec2-35-161-196-46.us-west-2.compute.amazonaws.com/api/get_label_st/"

    data = {'booking_id': booking['id']}
    headers = {'content-type': 'application/json'}
    response0 = requests.post(url, params={}, json=data, headers=headers)
    response0 = response0.content.decode('utf8').replace("'", '"')
    data0 = json.loads(response0)

    try:
        error = data0[0]['Error']
        print('#100, Get_Label failed:', booking['id'], error)
        save_error(booking['id'], error, mysqlcon)
    except KeyError:
        try:
            success_booking_id = data0[0]['Created Booking ID']
            print('#101, Get_Label success: ', success_booking_id)
            return True
        except KeyError:
            print('#102, Unknown Error: ', data0[0])

    # s0 = json.dumps(data0, indent=4, sort_keys=True)  # Just for visual
    # print(s0[0])

    return False

if __name__ == '__main__':
    print('#900 - Running %s' % datetime.datetime.now())

    try:
        mysqlcon = pymysql.connect(host=DB_HOST,
                                port=DB_PORT,
                                user=DB_USER,
                                password=DB_PASS,
                                db=DB_NAME,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)
    except:
        print('Mysql DB connection error!')
        exit(1)

    try:
        redisCon = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, charset="utf-8", decode_responses=True)
    except:
        print('Redis DB connection error!')
        exit(1)

    batch_size = get_status_update_batch_size(mysqlcon)
    bookings = get_available_bookings(mysqlcon, 2)

    if len(bookings) > 0:
        for booking in bookings:
            is_success = book(booking, mysqlcon)
            if is_success == True:
                time.sleep(4)
                get_label(booking, mysqlcon)
    else:
        print('#001 - No booing to be updated!')


    print('#901 - Finished %s' % datetime.datetime.now())
    mysqlcon.close()
