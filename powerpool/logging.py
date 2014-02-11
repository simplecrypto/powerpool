import traceback
import sys


class PrintLogger(object):
    def __init__(self, level=0):
        self.level = level

    def debug(self, *args, **kwargs):
        self.log(0, *args, **kwargs)

    def info(self, *args, **kwargs):
        self.log(10, *args, **kwargs)

    def warn(self, *args, **kwargs):
        self.log(20, *args, **kwargs)

    def error(self, *args, **kwargs):
        self.log(30, *args, **kwargs)

    def critical(self, *args, **kwargs):
        self.log(40, *args, **kwargs)

    def log(self, level, msg, exc_info=False):
        if self.level <= level:
            print(msg)
        if exc_info:
            print(traceback.format_exc())
