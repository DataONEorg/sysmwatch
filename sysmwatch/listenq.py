import sys
import logging
import json
import pika

R_URI = "amqp://USER:PASS@localhost:5672/%2F"

def getLogger():
    return logging.getLogger("listenq")

def callback(ch, method, properties, body):
    sysm = json.loads(body.decode())
    print(json.dumps(sysm, indent=2))

def main():
    L = getLogger()
    parameters = pika.URLParameters(R_URI)
    conn = pika.BlockingConnection(parameters)
    channel = conn.channel()
    channel.basic_consume(queue='sysm_watch', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
    return 0

if __name__ == "__main__":
    sys.exit(main())