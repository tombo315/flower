import json
import logging
from datetime import datetime
from socket import error as socketerror

import pg8000

logger = logging.getLogger(__name__)
connection = None
_connection_options = {}
skip_callback = False

REQ_MAX_RETRIES = 2

_all_tables = """
SELECT * FROM information_schema.tables
WHERE table_schema = 'public'
"""

_schema = (
    """CREATE TABLE events
    (
        id SERIAL PRIMARY KEY,
        time TIMESTAMP NOT NULL,
        data JSONB NOT NULL,
        unique (time, data)
    )""",
    "CREATE INDEX event_index ON events USING GIN (data)",
    "CREATE INDEX event_time_index ON events (time ASC)",
)

_add_event = """INSERT INTO events (time, data) VALUES (%s, %s) ON CONFLICT DO NOTHING"""

_get_events = """SELECT data FROM (
              SELECT *
              FROM events
              ORDER BY id DESC
              LIMIT {max_events}
              ) subevents
              ORDER BY time ASC
              """

_ignored_events = {
    'worker-offline',
    'worker-online',
    'worker-heartbeat',
}


def event_callback(state, event):
    if skip_callback or event['type'] in _ignored_events:
        return

    cursor = connection.cursor()
    retries_remaining = REQ_MAX_RETRIES
    while True:
        try:
            cursor.execute(_add_event, (
                datetime.fromtimestamp(event['timestamp']),
                json.dumps(event)
            ))
            connection.commit()
        except (socketerror, pg8000.InterfaceError):
            if retries_remaining > 0:
                logger.warning('Flower encountered a connection error with PostGreSQL database. Retrying.')
                open_connection(**_connection_options)
                cursor = connection.cursor()
                continue
            else:
                logger.exception('Flower encountered a connection error with PostGreSQL database. Unable to retry.')
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            return


def open_connection(user, password, database, host, port, use_ssl):
    global connection
    global _connection_options
    connection = pg8000.connect(
        user=user, password=password, database=database,
        host=host, port=port, ssl=use_ssl
    )
    _connection_options = {'user': user,
                           'password': password,
                           'database': database,
                           'host': host,
                           'port': port,
                           'use_ssl': use_ssl
                           }


def maybe_create_schema():
    global connection
    # Create schema if table is missing
    cursor = connection.cursor()
    try:
        cursor.execute(_all_tables)
        tables = cursor.fetchall()

        if tables is None or not any(('events' in table[2]) for table in tables):
            logger.debug('Table events missing, executing schema definition.')
            for statement in _schema:
                cursor.execute(statement)
            connection.commit()

    finally:
        cursor.close()


def close_connection():
    global connection
    if connection is not None:
        connection.close()
        connection = None


def get_events(max_events):
    logger.debug('Events loading from postgresql persistence backend')
    cursor = connection.cursor()
    retries_remaining = REQ_MAX_RETRIES
    while True:
        try:
            cursor.execute(_get_events.format(max_events=max_events))
            for row in cursor:
                yield row[0]
            logger.debug('Events loaded from PostGreSQL persistence backend')
        except (socketerror, pg8000.InterfaceError):
            if retries_remaining > 0:
                logger.warning('Flower encountered a connection error with PostGreSQL database. Retrying.')
                open_connection(**_connection_options)
                cursor = connection.cursor()
                continue
            else:
                logger.exception('Flower encountered a connection error with PostGreSQL database. Unable to retry.')
        finally:
            cursor.close()
            return
