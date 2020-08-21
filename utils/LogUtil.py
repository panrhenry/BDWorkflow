# coding=utf-8
"""
日志工具类
@author jiangbing
"""
import logging


class Logger(object):

    def __init__(self, filename, fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        logging.basicConfig(level=logging.INFO, format=fmt, filename=filename)
        self.logger = logging.getLogger()
