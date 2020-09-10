import logging
import threading
import queue
import datetime
import os

import can
import cantools
from influxdb import InfluxDBClient

idb = 'canbus2'

influxbufsize = 200

def influxwriter(msgqueue):
    influxdb = InfluxDBClient('localhost', 8086, '', '', idb)
    influxdb.create_database(idb)
    msgbuf = []

    while True:
        imsg = msgqueue.get()
        msgbuf.append(imsg)

        try:
            if len(msgbuf) >= influxbufsize:
                influxdb.write_points(msgbuf)
                msgbuf = []
        except:
            logging.exception('Error writing to influx')            

def create_json_body(timestamp, mgs, data):
    values = mgs.decode(data)
    
    json_body = {
            "measurement": f"{mgs.name}",
            "tags": {
                "car": "2016_CTS-V",
            },
            "time": timestamp,
            "fields": values 
    }

    return json_body


def main():
    jobtimestamp = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    logpath=os.getenv('CAN_LOGDIR')

    canbus ='vcan0'

    timestamp = datetime.datetime.now().strftime('%Y%M%d%h%m%s')

    db = cantools.database.load_file('/home/pi/canbus_dbc/gm_global_a_hs.dbc')
    msgqueue = queue.Queue()
    running = True

    iwriter = threading.Thread(target=influxwriter, args=(msgqueue, ))
    iwriter.start()

    vcan0 = can.Bus(canbus, bustype='socketcan')

    reader = can.BufferedReader()
    logger = can.Logger(f'{logpath}/{jobtimestamp}_{canbus}.log')

    listeners = [
        reader,
        logger
    ]

    notifier = can.Notifier(vcan0, listeners)

    try:
        while running:
            msg = reader.get_message()
            #if msg is None:
            #    running=False 

            try:
                m = db.get_message_by_frame_id(msg.arbitration_id)
                dt = datetime.datetime.fromtimestamp(msg.timestamp)
                
                jsonbody = create_json_body(dt, m, msg.data)
                logging.info(jsonbody) 

                msgqueue.put(jsonbody)

            except (KeyError, AttributeError) as e:
                pass
            except:
                logging.exception(f'Msg parse fail {m.name}')

            
    except:
        logging.exception('Timeout?')


    iwriter.join()
    notifier.stop()
    vcan0.shutdown()

if __name__ == '__main__':
    main()

