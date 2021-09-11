import sys
import logging
import signal
import pgnotify

def getLogger():
    return logging.getLogger("listen")

DB_URI = "postgresql://dataone_readonly:b6-7-CYTq7b@localhost:5433/metacat"

def main():
    L = getLogger()
    channels = ["sysm_watch", ]
    signals_to_handle = [signal.SIGINT, signal.SIGTERM]
    conn = pgnotify.get_dbapi_connection(DB_URI)
    for ev in pgnotify.await_pg_notifications(
        conn,
        channels,
        timeout=5,
        yield_on_timeout=True,
        handle_signals=signals_to_handle
    ):
        if isinstance(ev, int):
            sig = signal.Signals(ev)
            L.warning("Handling %s: stopping...", sig.name)
            break
        elif ev is None:
            L.debug("continue.")
        else:
            print(f"{ev.pid}:{ev.channel}:{ev.payload}")
    return 0

if __name__ == "__main__":
    sys.exit(main())