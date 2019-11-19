# -*- coding: utf-8 -*-
from logging.handlers import RotatingFileHandler
import logging
import argparse
import StringIO
import gzip
import os

from lxml import etree

def build_logger(name="rrc_validation_assisant"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        file_name = os.path.normpath(os.path.join('.', name+'.log'))
        fh = RotatingFileHandler(file_name, mode="a", maxBytes=100*1024*1024, backupCount=10, encoding=None, delay=0)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger

class CheckUT(object):
    UT_XPATH = "/REC/static_data/summary/EWUID/@uid"

    def __init__(self, data_path):
        super(CheckUT, self).__init__()
        self._data_path = os.path.normpath(os.path.realpath(os.path.abspath(data_path)))
        self.logger = build_logger()

    def extract_xml(self, ut):
        for root, subdir, files in os.walk(self._data_path):
            for f in files:
                if not f.endswith('.gz'):
                    continue

                p_ = os.path.normpath(os.path.join(root, f))
                self.logger.debug(p_)

                with gzip.open(p_, 'rb') as gzfile:
                    for rec in gzfile:
                        if not len(rec):
                            continue

                        tree = etree.parse(StringIO.StringIO(rec))
                        uts = tree.xpath(CheckUT.UT_XPATH)
                        for ut_ in uts:
                           # self.logger.debug(p_)
                            if ut == ut_:
                                self.logger.info(p_)
                                d = ut.rpartition(':')[2] + '.xml'
                                self.logger.info(d)
                                with open(d, 'w') as r:
                                    r.write(rec)

                                return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--ut', help='The ut you want to extract', required=True, default="")
    parser.add_argument('-d', '--data_dir', help='The directory path where the xml gzip files are',
                        required=False, default=None)
    args = parser.parse_args()

    checker = CheckUT(args.data_dir)
    checker.extract_xml(args.ut)
