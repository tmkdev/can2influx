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

    running = True

    while running:
        imsg = msgqueue.get()
        if imsg == -1:
            running=False
            influxdb.write_points(msgbuf)
            continue

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

    canbus = os.getenv('canbus', 'vcan0')
    logging.warning(f'using canbus {canbus}')

    timestamp = datetime.datetime.now().strftime('%Y%M%d%h%m%s')

    db = cantools.database.load_file('/home/pi/canbus_dbc/gm_global_a_hs.dbc')

    msgqueue = queue.Queue()

    iwriter = threading.Thread(target=influxwriter, args=(msgqueue, ))
    iwriter.start()

    bus = can.Bus(canbus, bustype='socketcan')

    reader = can.BufferedReader()
    logger = can.Logger(f'{logpath}/{jobtimestamp}_{canbus}.log')

    listeners = [
        reader,
        logger
    ]

    notifier = can.Notifier(bus, listeners)

    packetcount=0
    try:
        while True:
            msg = reader.get_message()
            packetcount+=1

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

            if packetcount % 20000 == 0:
                logging.warning(f'Processed canbus {packetcount} packets')

            
    except KeyboardInterrupt:
        msgqueue.put(-1)
    except:
        logging.exception('Unhandled message parse Error')

    iwriter.join()
    notifier.stop()
    bus.shutdown()

if __name__ == '__main__':
    main()

