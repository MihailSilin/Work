# *-* coding: utf-8 *-*
from elasticsearch import Elasticsearch
import elasticsearch_dsl as es_dsl
import os
import xml.etree.cElementTree as et
import json
import argparse

analysis = { "filter": 
                {"bigram_filter": 
                    {"type":             "shingle",
                    "min_shingle_size": 2, 
                    "max_shingle_size": 2, 
                    "output_unigrams":  False   
                    }
                },
            
            "analyzer" : 
                {"bigram_analyzer": 
                    {"type":             "custom",
                    "tokenizer":        "standard",
                    "filter": [
                        "lowercase",
                        "bigram_filter" 
                    ]
                    }
                }
            }

bigramField = {
    "type":     "text",
    "analyzer": "bigram_analyzer"
}

class queryTipsIndex_doctype(es_dsl.DocType):
        phrase = es_dsl.Text(fields = {"bigrams":bigramField})
        norm_phrase = es_dsl.Text(fields = {"bigrams":bigramField})
        pt_id = es_dsl.Text()
        doc_id = es_dsl.Integer()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create or reupload index')
    parser.add_argument('--input', type=str,
                        help='Json file with preprocessed data')
    parser.add_argument('--index', type=str,
                        help='Index name to create')
    args = parser.parse_args()

    indexName = args.index  # "query_tips_index_v0.3"
    path = args.input  # "../data/data/data_clear_es_pm.json"
    properties = ["request", "request_lemms", "popularity"] #without id

    esClient = Elasticsearch()
    index = es_dsl.Index(indexName, using=esClient)

    index.delete(ignore=404)
    index.settings(number_of_shards=1, number_of_replicas=0, analysis = analysis)
    index.doc_type(queryTipsIndex_doctype)  #<-- CHANGE NAME
    index.create()
    queryTipsIndex_doctype.init(using=esClient)

    i = 0
    with open(path, 'r', encoding = 'utf-8') as fn:
        data = json.load(fn)

    #json имеет вид: [1:{original:.. norm:.. llt_id:.. pt_id:..} 2:...]
    for i, d in enumerate(data):
        if i % 8000 == 0:
            print(i)
        doc = queryTipsIndex_doctype()
        setattr(doc, "phrase", d['original'])
        setattr(doc, "norm_phrase", d['norm'])
        setattr(doc, "pt_id", d['pt_id'])
        doc.doc_id = i
        doc.save(using=esClient)
        '''
        try:
            doc = uno_gramsIndex_doctype()
            setattr(doc,"req",req)
            setattr(doc,"norm_req",norm_req)
            doc.doc_id = i
            doc.save(using=esClient)
        except Exception, e:
            print("Error in chat ID : {0}".format(str(i)))
        '''
    print ("all good!")
