from __future__ import absolute_import
from __future__ import with_statement

import time
import shelve
import logging
import threading
import collections

from functools import partial

import celery

from tornado.ioloop import PeriodicCallback
from tornado.ioloop import IOLoop

from celery.events import EventReceiver
from celery.events.state import State

from . import api

try:
    from collections import Counter
except ImportError:
    from .utils.backports.collections import Counter


logger = logging.getLogger(__name__)


class EventsState(State):
    # EventsState object is created and accessed only from ioloop thread

    def __init__(self, *args, **kwargs):
        super(EventsState, self).__init__(*args, **kwargs)
        self.counter = collections.defaultdict(Counter)

    def event(self, event, websockets=True):
        worker_name = event['hostname']
        event_type = event['type']

        self.counter[worker_name][event_type] += 1

        if websockets:
            # Send event to api subscribers (via websockets)
            classname = api.events.getClassName(event_type)
            cls = getattr(api.events, classname, None)
            if cls:
                cls.send_message(event)

        # Save the event
        super(EventsState, self).event(event)


class Events(threading.Thread):
    events_enable_interval = 5000

    def __init__(self, capp, db=None, persistent=False,
                 enable_events=True, io_loop=None, storage_driver=None,
                 storage_max_events=None, **kwargs):
        threading.Thread.__init__(self)
        self.daemon = True

        self.io_loop = io_loop or IOLoop.instance()
        self.capp = capp

        self.db = db
        self.persistent = persistent
        self.storage_driver = storage_driver
        self.storage_max_events = storage_max_events
        self.enable_events = enable_events
        self.state = None

        if self.persistent and tuple(map(int, celery.__version__.split('.'))) < (3, 0, 15):
            logger.warning('Persistent mode is available with '
                           'Celery 3.0.15 and later')
            self.persistent = False

        if self.persistent:
            if storage_driver == 'file':
                logger.debug("Loading state from '%s'...", self.db)
                state = shelve.open(self.db)
                if state:
                    self.state = state['events']
                state.close()

            elif storage_driver == 'postgres':
                from flower.utils import pg_storage
                self.state = EventsState(
                    callback=pg_storage.event_callback, **kwargs
                )

                # When loading past events, do not call the event callback
                # Need to do it like this instead of overriding the callable
                # because the callable is cached in the closure of
                # celery.events.state.State._create_dispatcher
                pg_storage.skip_callback = True

                try:
                    for event in pg_storage.get_events(max_events=self.storage_max_events):
                        self.state.event(event, websockets=False)
                finally:
                    pg_storage.skip_callback = False

        if not self.state:
            self.state = EventsState(**kwargs)

        self.timer = PeriodicCallback(self.on_enable_events,
                                      self.events_enable_interval)

    def start(self):
        threading.Thread.start(self)
        # Celery versions prior to 2 don't support enable_events
        if self.enable_events and celery.VERSION[0] > 2:
            self.timer.start()

    def stop(self):
        if self.persistent and self.storage_driver == 'file':
            logger.debug("Saving state to '%s'...", self.db)
            state = shelve.open(self.db)
            state['events'] = self.state
            state.close()

    def run(self):
        try_interval = 1
        while True:
            try:
                try_interval *= 2

                with self.capp.connection() as conn:
                    recv = EventReceiver(conn,
                                         handlers={"*": self.on_event},
                                         app=self.capp)
                    try_interval = 1
                    recv.capture(limit=None, timeout=None, wakeup=True)

            except (KeyboardInterrupt, SystemExit):
                try:
                    import _thread as thread
                except ImportError:
                    import thread
                thread.interrupt_main()
            except Exception as e:
                logger.error("Failed to capture events: '%s', "
                             "trying again in %s seconds.",
                             e, try_interval)
                logger.debug(e, exc_info=True)
                time.sleep(try_interval)

    def on_enable_events(self):
        # Periodically enable events for workers
        # launched after flower
        try:
            self.capp.control.enable_events()
        except Exception as e:
            logger.debug("Failed to enable events: '%s'", e)

    def on_event(self, event):
        # Call EventsState.event in ioloop thread to avoid synchronization
        self.io_loop.add_callback(partial(self.state.event, event))
