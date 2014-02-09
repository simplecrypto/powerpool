class PrintLogger(object):
    def __init__(self, level=0):
        self.level = level

    def debug(self, msg):
        self.log(0, msg)

    def info(self, msg):
        self.log(10, msg)

    def warn(self, msg):
        self.log(20, msg)

    def error(self, msg):
        self.log(30, msg)

    def critical(self, msg):
        self.log(40, msg)

    def log(self, level, msg):
        if self.level <= level:
            print(msg)
