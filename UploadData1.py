# *-* coding: utf-8 *-*
from elasticsearch import Elasticsearch
import elasticsearch_dsl as es_dsl
import os
import xml.etree.cElementTree as et
import json
import argparse
import re
from pymorphy2 import MorphAnalyzer

morph = MorphAnalyzer()

def normalize_text(inp_text):
    if (str(inp_text) == ''):
        inp_text = ""
    l_sent = []
    words = inp_text.replace(',', '')
    words = words.replace('-', ' ')
    words = words.split(' ')
    for word in words:
        try:
            parsResult = morph.parse(word)[0]
            lemm = parsResult.normal_form.upper()
            d_word = lemm
            l_sent.append(d_word)
        except Exception as e:
            l_sent.append(word)
            raise (e)
            # logger.error("Слово без леммы: {0}".format(word))
    return l_sent

analysis = {"filter":
                {"bigram_filter":
                     {"type": "shingle",
                      "min_shingle_size": 2,
                      "max_shingle_size": 2,
                      "output_unigrams": False
                      },
                 },

            "analyzer":
                {"bigram_analyzer":
                     {"type": "custom",
                      "tokenizer": "standard",
                      "filter": [
                          "lowercase",
                          "bigram_filter"
                      ]
                      },
					  "my_analyzer": {
          "tokenizer": "my_tokenizer"
        }
                 } ,
				 
				"tokenizer": {
        "my_tokenizer": {
          "type": "ngram",
          "min_gram": 2,
          "max_gram": 10    ,
          "token_chars": [
            "letter",
            "digit"
          ]
				 

            }
			}
}
bigramField = {
    "type": "text",
    "analyzer": "bigram_analyzer"
}


class queryTipsIndex_doctype(es_dsl.DocType):
    phrase = es_dsl.Text(fields={"bigrams": bigramField})
    norm_phrase = es_dsl.Text(fields={"bigrams": bigramField})
	
    pt_id = es_dsl.Text()
    doc_id = es_dsl.Integer()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create or reupload index')
    # parser.add_argument('--input', type=str,
    #                     help='Json file with preprocessed data')
    parser.add_argument('-i', '--index', type=str,
                        help='Index name to create')
    args = parser.parse_args()

    indexName = args.index  # "query_tips_index_v0.3"
    # path = args.input  # "../data/data/data_clear_es_pm.json"
    properties = ["request", "request_lemms", "popularity"]  # without id

    esClient = Elasticsearch()
    index = es_dsl.Index(indexName, using=esClient)

    index.delete(ignore=404)
    index.settings(number_of_shards=1, number_of_replicas=0, analysis=analysis)
    index.doc_type(queryTipsIndex_doctype)  # <-- CHANGE NAME
    index.create()
    queryTipsIndex_doctype.init(using=esClient)

    with open('./vidal_total_dict.json', 'r', encoding='utf-8') as f_v:
        vidal_data = json.load(f_v)

    i=0
    # json имеет вид: [1:{original:.. norm:.. llt_id:.. pt_id:..} 2:...]
    for elem in vidal_data.items():
        # print(normalize_text(elem[0]))
        # if i % 800 == 0:
        #     print(i)
        doc = queryTipsIndex_doctype()
        drug = re.sub("<SUP>&trade;</SUP>|<SUP>&reg;</SUP>", "", elem[0])
        drug = re.sub("<SUP>|</SUP>", "", drug)
        setattr(doc, "drug", drug)

        normalized_drug = ' '.join(normalize_text(drug))
        # print(normalized_drug)
        setattr(doc, "norm_drug", normalized_drug)
        setattr(doc, "INN", elem[1]['МНН'])
        setattr(doc, "ATC", elem[1]['Код АТХ'])
        if i % 500 == 0:
            print(drug)
            print(normalized_drug)
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
        i+=1
    print ("all good!")