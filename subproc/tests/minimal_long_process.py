import signal
import os
import sys
import time

import logging

from subproc.subprocess_protocol_implement import SubprocessProtocolImplement

protocol = SubprocessProtocolImplement(logging.getLogger(__name__))

protocol.started_event()
#print('{0} -STARTED-'.format(os.getpid()))


def shutdown(signum, frame=None):
    protocol.shutdown_event(exit_code=-signum)
    #print('{signum} triggered -SHUTDOWN-'.format(**locals()))
    sys.stdout.flush()
    sys.exit(-signum)  # using a convention already seen somewhere else... (REF ?)

signal.signal(signal.SIGTERM, shutdown)

if signal.getsignal(signal.SIGINT) is signal.SIG_IGN:  # if sigint is ignored (python didnt setup default_int_handler)
    signal.signal(signal.SIGINT, shutdown)  # to get Ctrl-C trigger shutdown, even in child process

# NOTE : SIGKILL cannot be caught

try:
    # do not block for ever, so that, in case something is broken in test, we can see it.
    time.sleep(10)
except KeyboardInterrupt as ki:
    # CAREFUL this is not caught in child process (only top parent), but SIGINT will still be triggered.
    # REF : https://stackoverflow.com/questions/40775054/capturing-sigint-using-keyboardinterrupt-exception-works-in-terminal-not-in-scr/40785230#40785230
    print('KeyboardInterrupt')
    shutdown(signal.SIGINT)

shutdown(0)

