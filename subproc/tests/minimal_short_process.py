import signal
import os

import logging

from subproc.subprocess_protocol_implement import SubprocessProtocolImplement

protocol = SubprocessProtocolImplement(logging.getLogger(__name__))

protocol.started_event()
#print('{0} -STARTED-'.format(os.getpid()))


def shutdown(signum, frame):
    protocol.shutdown_event(exit_code=-signum)
    #print('{signum} triggered -SHUTDOWN-'.format(**locals()))

signal.signal(signal.SIGTERM, shutdown)

# do not do anything to exit immediately
protocol.shutdown_event(0)

