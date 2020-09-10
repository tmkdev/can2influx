import re
import binascii
import struct
import logging
import datetime

import can
import cantools

from influxdb import InfluxDBClient

client = InfluxDBClient('localhost', 8086, '', '', 'canbus2')
client.create_database('canbus2')

db = cantools.database.load_file('/home/pi/canbus_dbc/gm_global_a_hs.dbc')

RE_CANDUMP_LOG = re.compile(r'^\((\d+\.\d+)\)\s+\S+\s+([\dA-F]+)#([\dA-F]*)$')

def mo_unpack(mo):
    timestamp = float(mo.group(1))
    frame_id = mo.group(2)
    frame_id = '0' * (8 - len(frame_id)) + frame_id
    frame_id = binascii.unhexlify(frame_id)
    frame_id = struct.unpack('>I', frame_id)[0]
    data = mo.group(3)
    data = data.replace(' ', '')
    data = binascii.unhexlify(data)

    return timestamp, frame_id, data

def create_json_body(timestamp, mgs, data):
    values = m.decode(data)
    
    json_body = {
            "measurement": f"{m.name}",
            "tags": {
                "car": "2016_CTS-V",
            },
            "time": timestamp,
            "fields": values 
    }

    return json_body



linecount = 0
payload = []

with open('/home/pi/log/fulllog.log', 'r') as canlog:
    for line in canlog:
        m = RE_CANDUMP_LOG.search(line)

        if m:
            timestamp, id, data = mo_unpack(m)

            msg = can.Message(arbitration_id=id,
                              data=data,
                              is_extended_id=False)
            m = None
            try:
                m = db.get_message_by_frame_id(id)
                dt = datetime.datetime.fromtimestamp(timestamp)
                
                jsonbody = create_json_body(dt, m, data)
                print(jsonbody) 
            except:
                pass


            try:
                if m is not None:
                    if jsonbody['fields'] != {}:
                        payload.append(jsonbody)

                        if len(payload) > 5000:
                            logging.warning('Writing batch to influx')
                            client.write_points(payload)
                            payload = []
                    
            except:
                logging.exception('Error- ')

            linecount+=1
            if not linecount % 10000:
                print(f"Processed {linecount} lines")


