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
    wos_dais_ng_id_fmt += " \"_source\":[\"daisngids\",\"citingsrcslocalcount\", \"processingtime\"], "
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
                print("*"*100)
                print(query_string)
                print("*"*100)

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





if __name__ == '__main__':
    ids_file = './20191205-incrementals-1575503938-ut-in-author-not-in-wos-dais_ng_id.txt'
    es_request = QueryAssistant()
    # es_request.buid_wos_dais_id_request(ids_file)
    es_request.build_author_dais_id_request(ids_file)