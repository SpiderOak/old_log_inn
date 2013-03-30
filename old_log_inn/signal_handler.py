# -*- coding: utf-8 -*-
"""
signal_handler.py

a function to set a signal handler for a process
"""
import signal 
from threading import Event

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def set_signal_handler():
    """
    set a signal handler to set halt_event when SIGTERM is raised
    return halt_event
    """
    halt_event = Event()
    signal.signal(signal.SIGTERM, _create_signal_handler(halt_event))
    return halt_event
