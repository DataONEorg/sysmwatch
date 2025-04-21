# sysmwatch

This python tool watches the postgres systemmetadata tables, pulling out entries
with dateModified more recent than a specified value.

Identifiers are examined in the Solr index, and flagged if the indexed dateModified 
does not match that of the systemMetadata.

The process is fairly efficient and may provide a basis for implementing
a replacement for the index-task-generator which currently relies on hazelcast events.

Output is to a JSON file that can be rendered with a simple handsontable implemnetation.

## Approaches

There are several approaches to postgres event notification:

1. Polling. Periodic query to retrieve a (hopefully) limited set of changes. Implemented in `main.py`.
2. Listening for `pg_notify` events. Implemented in `listen.py`
3. Using a postgres extension to call a amqp message queue. Listener in `listenq.py`
4. Write events to a queue table (e.g. using a trigger) and retrieve events by querying the table using the `SKIP LOCKED` semantics. Not implemented here, some discussion on [stack overflow](https://stackoverflow.com/questions/297280/the-best-way-to-use-a-db-table-as-a-job-queue-a-k-a-batch-queue-or-message-queu). Simple example on [HN](https://news.ycombinator.com/item?id=20020501).
