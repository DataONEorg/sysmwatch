import logging
import click
import os
import sysmwatch
import json
import datetime
import signal
import time
import requests
import progress.bar
import dateparser

WAIT_TIME = 30


class SIGINT_handler:
    def __init__(self):
        self.SIGINT = False

    def signal_handler(self, signal, frame):
        print("Exiting...")
        self.SIGINT = True


handler = SIGINT_handler()
signal.signal(signal.SIGINT, handler.signal_handler)


def passFromPGPass(pgpass="~/.pgpass", database=sysmwatch.METACAT_DB):
    pgpass = os.path.expanduser(pgpass)
    lines = open(pgpass, "r").read().split("\n")
    for line in lines:
        parts = line.split(":")
        db = parts[2]
        if db == database:
            return parts[-1]
    return None


class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return sysmwatch.datetimeToJsonStr(obj)
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)


def printReport(report):
    print(json.dumps(report, indent=2, cls=DatetimeEncoder))


def watch(conn, oldest):
    session = requests.Session()
    while True:
        if handler.SIGINT:
            break
        report = sysmwatch.generateReport(conn, oldest)
        printReport(report)
        oldest = report["t_oldest_bad"]
        break
        with progress.bar.Bar("Pause", max=WAIT_TIME) as bar:
            for i in range(WAIT_TIME):
                time.sleep(1)
                bar.next()
                if handler.SIGINT:
                    break
    conn.close()

@click.command()
@click.option("-o","--oldest",default="midnight UTC", help="Oldest date to start from")
@click.option("-p", "--pgpass", default="~/.pgpass", help="pgpass file")
@click.option("--port", default=sysmwatch.METACAT_PORT, help="Port for postgres")
def main(oldest, pgpass, port):
    oldest = dateparser.parse(oldest, settings={"TIMEZONE": "+0000", "RETURN_AS_TIMEZONE_AWARE":False})
    password = passFromPGPass(pgpass)
    conn = sysmwatch.connectMetacat(port=port, password=password)
    watch(conn, oldest)


if __name__ == "__main__":
    main()
