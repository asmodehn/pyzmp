import os
import re
import signal

import six

try:
    import asyncio
except ImportError:
    import trollius as asyncio

import logging.handlers

"""
This is a text-based (ASCII) protocol, for communicating and synchronizing between processes, through pipes.
The goal is to synchronize process startup / shutdown behavior to be able to implement synchronization at a higher level...

Note this does the same job as what is found in systemd, upstart, openrc, etc.
See https://en.wikipedia.org/wiki/Init
TODO : compatibility with those, eventually 

"""


from .subprocess_protocol import STARTED_FMT, SHUTDOWN_FMT


class SubprocessProtocolImplement(object):
    """helper for a process to implement the protocol.
    """

    # Ref : https://stackoverflow.com/questions/17558552/how-do-i-add-custom-field-to-python-log-format-string
    # to add extra fields to the log format...

    def __init__(self, logger):
        self.logger = logger
        self.prepend_format_str = '[%(process)d %(threadName)s %(relativeCreated)d]'

        ensure_console = False
        ensure_syslog = False
        for h in self.logger.handlers:
            if isinstance(h, logging.StreamHandler):  # we have one stream handler
                ensure_console = True
                # We just modify the existing format (no API for this ?)
                h.setFormatter(logging.Formatter(fmt=self.prepend_format_str + h.formatter._fmt, datefmt=h.formatter.datefmt, style=h.formatter._style))

            if isinstance(h, logging.handlers.SysLogHandler):  # we have one syslog handler
                ensure_syslog = True
                # We just modify the existing format (no API for this ?)
                h.setFormatter(logging.Formatter(fmt=self.prepend_format_str + h.formatter._fmt, datefmt=h.formatter.datefmt, style=h.formatter._style))

        # enforce minimum handlers
        if not ensure_console:
            # configure stream handler
            consoleHandler = logging.StreamHandler()
            consoleHandler.setFormatter(logging.Formatter(self.prepend_format_str + logging.BASIC_FORMAT))
            self.logger.addHandler(consoleHandler)

        if not ensure_syslog:
            # configure syslog handler
                # TODO : add syslog config in /etc/rsyslog.d/ on install/startup...
            syslogHandler = logging.handlers.SysLogHandler(address='/dev/log')
            syslogHandler.setFormatter(logging.Formatter(self.prepend_format_str + ' %(filename)s:%(lineno)d ' + logging.BASIC_FORMAT))
            self.logger.addHandler(syslogHandler)

        if not self.logger.isEnabledFor(logging.INFO):  # we need to be enabled at minimum for info
            self.logger.setLevel(logging.INFO)

    def started_event(self):
        """Needs to be called after startup, once all initialization has been done.
        """
        self.logger.info(STARTED_FMT.format(pid=os.getpid()))

    def shutdown_event(self, exit_code):
        """Needs to be called before shutdown, before cleaning up.
        """
        if exit_code < 0:
            for s in signal.Signals:
                if s.value == -exit_code:
                    exit_code = s.name
                    break  # we found it and changed exit_code to str.

        self.logger.info(SHUTDOWN_FMT.format(exit_code=exit_code))



