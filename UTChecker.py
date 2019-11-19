# -*- coding: utf-8 -*-
from lxml import etree

class CheckUT:
    def __init__(self):
        super(CheckUT, self).__init__()

    def ut_in_dais_baseline(self, author_index_ut, dais_baseline_ut):
        with open(author_index_ut, 'r') as f1:
            with open(dais_baseline_ut, 'r') as f2:
                ut_in_index = set([i.strip() for i in f1])
                ut_in_baseline = set([i.strip() for i in f2])
                print(ut_in_index)
                print(ut_in_baseline)
                # union = ut_in_index & ut_in_baseline
                print(ut_in_index-ut_in_baseline)
                print(ut_in_baseline-ut_in_index)

    def extract(self, xpath, msg=None):
        tree = etree.parse('D:\\dev\\rrc validation\\ut.xml')
        nodes = tree.xpath(xpath)
        if len(nodes):
            with open('dais_ng_ids.txt', 'w') as r:
                msg and r.write("# {0}\n\n".format(msg))
                for i in nodes:
                    r.write(i+'\n')

    def extract_xml(self):
        pass


if __name__ == '__main__':
    checker = CheckUT()
    checker.extract("/REC/static_data/summary/names/name[@role='author' or @role='anon'][@lang_id='en' or not(@lang_id)][@display='Y' or not(@display)]/@daisng_id",
                    "These daisng_ids have the ut WOS:000209230400010.")