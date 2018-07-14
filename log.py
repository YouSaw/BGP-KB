from main import *
import logging

"""
Used for logging
"""
logFormatter = logging.Formatter("%(asctime)-10s %(levelname)-6s %(message)-10s", datefmt='%d %H:%M:%S')
rootLogger = logging.getLogger()

fileHandler = logging.FileHandler("{0}/{1}.log".format("logs", session_name))
fileHandler.setFormatter(logFormatter)
fileHandler.setLevel(logging.DEBUG)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(logging.INFO)
rootLogger.addHandler(consoleHandler)
rootLogger.setLevel(logging.DEBUG)

