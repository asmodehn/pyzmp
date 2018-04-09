"""Verify that the minimal_long_process behave as expected in all cases.
This is not related to the protocol, but related to other external/system events (signals, interruptions, etc.)
"""
import os
import signal
import subprocess

# Ref : https://stackoverflow.com/questions/40775054/capturing-sigint-using-keyboardinterrupt-exception-works-in-terminal-not-in-scr/40785230#40785230
import threading
import time

# TODO : change into logging protocol adapter test

def test_shutdown_triggers_from_main():
    mpath = os.path.join(os.path.dirname(__name__), 'minimal_long_process.py')
    proc = subprocess.Popen(['python', mpath], stdin=None, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    def late_killer(p):
        time.sleep(2)  # sleep a bit so main task has time to communicate()
        p.send_signal(signal.SIGINT)

    t = threading.Thread(target=late_killer, args=(proc,))
    t.start()

    out, _ = proc.communicate()

    assert '-STARTED-'.encode() in out, print(out)
    assert '-SHUTDOWN SIGINT-'.encode() in out, print(out)
    assert proc.returncode == -2 % 256, print(proc.returncode)  # check signal is returned

def test_shutdown_triggers_from_attached_child():
    mpath = os.path.join(os.path.dirname(__name__), 'minimal_long_process.py')
    proc = subprocess.Popen('python '+mpath, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

    def late_killer(p):
        time.sleep(2)  # sleep a bit so main task has time to communicate()
        p.send_signal(signal.SIGINT)

    t = threading.Thread(target=late_killer, args=(proc,))
    t.start()

    out, _ = proc.communicate()

    assert '-STARTED-'.encode() in out, print(out)
    assert '-SHUTDOWN SIGINT-'.encode() in out, print(out)
    assert proc.returncode == -2 % 256, print(proc.returncode)  # check signal is returned

def test_shutdown_triggers_from_detached_child():
    mpath = os.path.join(os.path.dirname(__name__), 'minimal_long_process.py')
    proc = subprocess.Popen('python '+mpath+' &', stdin=None, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

    def late_killer(p):
        time.sleep(2)  # sleep a bit so main task has time to communicate()
        p.send_signal(signal.SIGINT)

    t = threading.Thread(target=late_killer, args=(proc,))
    t.start()

    out, _ = proc.communicate()

    assert '-STARTED-'.encode() in out, print(out)
    assert '-SHUTDOWN SIGINT-'.encode() in out, print(out)
    assert proc.returncode == -2 % 256, print(proc.returncode)  # check signal is returned