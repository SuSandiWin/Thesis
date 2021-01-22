from __future__ import print_function
from time import sleep
import re
import json
import argparse
import sys
from kafka import KafkaProducer


def send_message(producer, topic, input):
    with open(input, 'r') as ins:
        for line in ins:
            line=line.strip()
            value1=line.split(';')
            #value1 = map(''.join, re.findall(r'(.*?)(?:;|\\Z)', line))
            #producer.send(topic,value1)
            producer.send(topic, json.dumps({'Date': value1[0], 'Time': value1[1], 'Global_active_power': value1[2], 'Global_reactive_power': value1[3],'Voltage': value1[4], 'Global_intensity': value1[5], 'Sub_metering_1': value1[6],'Sub_metering_2': value1[7], 'Sub_metering_3': value1[8]}))
            sleep(1)
            print("Success")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-rh', '--host', default="localhost:9092")
    parser.add_argument('-t', '--topic', default='kafkathird')
    parser.add_argument('-i', '--input', required=True)
    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.host)
    send_message(producer, args.topic, args.input)