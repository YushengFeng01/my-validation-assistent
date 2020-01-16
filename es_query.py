# -*- coding: utf-8 -*-
import subprocess
import json

cluster_1 = {
    'server': 'elasticsearch.cluster_2.wos.eu-west-1.prod1.prod.oneplatform.build:9200',
    'index': 'author',
    'daisngids': '1191407'
}

def build_wos_dais_ng_id_fmt(cluster):
    wos_dais_ng_id_fmt = "curl -s -H 'Content-Type: application/json'"
    wos_dais_ng_id_fmt += " -XPOST '%s/wos/_search?pretty' -d '{ "%(cluster['server'])
    wos_dais_ng_id_fmt += " \"_source\":[\"daisngids\",\"citingsrcslocalcount\", \"processingtime\", \"colluid\"], "
    wos_dais_ng_id_fmt += " \"query\" : { "
    wos_dais_ng_id_fmt += " \"bool\": { "
    wos_dais_ng_id_fmt += " \"must\": [ { \"match\" : {\"daisngids\" : %s} } ]}}}' "%(cluster['daisngids'])

    return wos_dais_ng_id_fmt

def build_author_dais_id_fmt(cluster):
    authorship_fmt = "curl -s -H 'Content-Type: application/json'"
    authorship_fmt += " -XPOST '%s/author/_search?pretty' -d "%(cluster['server'])
    authorship_fmt += " '{\"_source\":[\"authorships.ut\",\"author_name\", \"processingtime\"], "
    authorship_fmt += " \"query\":{ "
    authorship_fmt += " \"term\" : { \"dais_ng_id\":\"%s\"}}}' "%(cluster['daisngids'])

    return authorship_fmt

def build_ut_fmt(cluster, ut):
    ut_fmt = "curl -s -H 'Content-Type: application/json'"
    ut_fmt += " -XPOST '%s/wos/_search?pretty' -d " % (cluster['server'])
    ut_fmt += " '{\"_source\":[\"citingsrcslocalcount\",\"daisngids\", \"processingtime\", \"colluid\"], "
    ut_fmt += " \"query\":{ "
    ut_fmt += " \"bool\" : { \"must\" : [ { \"match\" : { \"colluid\":\"%s\"}}]}}}' " % (ut)

    return ut_fmt


class QueryAssistant(object):
    def __init__(self):
        super(QueryAssistant, self).__init__()


    def send_request(self, request):
        response = subprocess.check_output(request, stderr=subprocess.PIPE, shell=True, universal_newlines=False)
        return json.loads(response, encoding='utf-8')

    def buid_wos_dais_id_request(self, path):
        dais_ids = []
        with open(path, 'r') as src:
            dais_ids = [i.strip() for i in src if not i.startswith('Author_id')]

        for id in dais_ids:
            cluster_1['daisngids'] = id
            query_string = build_wos_dais_ng_id_fmt(cluster_1)
            response = self.send_request(query_string)
            hits = response['hits']['total']
            # print(response)
            if hits != 0:
                print("id {0} is still in wos index".format(id))

    def build_author_dais_id_request(self, path):
        dais_ids = []
        with open(path, 'r') as src:
            dais_ids = [i.strip() for i in src if not i.startswith('Author_id')]

        with open('author_ids.txt', 'w') as dst:
            for id in dais_ids:
                cluster_1['daisngids'] = id
                query_string = build_author_dais_id_fmt(cluster_1)
                response = self.send_request(query_string)
                total = response['hits']['total']
                if total > 0:
                    hits = response['hits']['hits']
                    processingtime = hits[0]['_source']['processingtime']
                    dst.write(id+'\t'+processingtime+'\n')

    def build_ut_wos_request(self, path):
        with open(path, 'r') as src:
            for i in src:
                if i.startswith("Author_id") or len(i) < 1:
                    continue
                ut = i.split()[1]
                query_string = build_ut_fmt(cluster_1, ut)
                response = self.send_request(query_string)
                total = response['hits']['total']
                if total < 1:
                    print("ut {0} isn't in wos index now".format(ut))


    def build_ut_counts_difference(self, path):
        with open(path, 'r') as src:
            for i in src:
                if i.startswith('Author_id') or len(i) < 1:
                    continue

                cluster_1['daisngids'] = i.strip()

                query_string = build_author_dais_id_fmt(cluster_1)
                #print("author query: {0}".format(query_string))
                response = self.send_request(query_string)
                total = response['hits']['total']
                if total > 0:
                    hits = response['hits']['hits']
                    authorship = hits[0]['_source']["authorships"]
                    ut_in_author = set([l['ut'].strip() for l in authorship])
                else:
                    print("id {0} isn't in author index now".format(i.strip()))

                query_string = build_wos_dais_ng_id_fmt(cluster_1)
                #print("wos query: {0}".format(query_string))
                response = self.send_request(query_string)
                total = response['hits']['total']
                if total > 0:
                    hits = response['hits']['hits']
                    ut_in_wos = set([h['_source']['colluid'][0].strip() for h in hits])
                else:
                    print("id {0} isn't in wos index now".format(i.strip()))

                print("id {0}, ut_in_author_not_in_wos {1}".format(i.strip(), ut_in_author-ut_in_wos))
                print("id {0}, ut in author and wos {1}".format(i.strip(), ut_in_wos&ut_in_author))
                print("="*100)
                ut_in_author.clear()
                ut_in_wos.clear()


if __name__ == '__main__':
    ids_file = './incrementals-1579046338-ut-in-author-not-in-wos-dais_ng_id.txt'
    uts_file = './incrementals-1579046338-ut-in-author-not-in-wos.txt'
    es_request = QueryAssistant()
    es_request.buid_wos_dais_id_request(ids_file)
    es_request.build_author_dais_id_request(ids_file)
    es_request.build_ut_wos_request(uts_file)
    es_request.build_ut_counts_difference(ids_file)
