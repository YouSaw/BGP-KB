from main import *
import logging

"""
Used for logging
"""
logFormatter = logging.Formatter("%(asctime)-10s %(levelname)-6s %(message)-10s", datefmt='%d %H:%M:%S')
rootLogger = logging.getLogger()
rootLogger.level = logging.INFO

fileHandler = logging.FileHandler("{0}/{1}.log".format("logs", session_name))
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

