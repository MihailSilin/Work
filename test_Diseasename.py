import elasticsearch
# import elasticsearch_dsl as es_dsl
import os
import sys
from sklearn.metrics import classification_report, precision_recall_fscore_support
import json
import re
# -*- coding: utf-8 -*-

BIGRAM_QUERY_TEMPLATE = {
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "norm_phrase.bigrams": {
                            "query": "",
                            "fuzziness": 1,
                        },
                    }
                }
            }
        },
        "size": 5,
        "sort": {
            "_score": {
                "order": "desc"
            }
        },
    }

WORD_QUERY_TEMPLATE = {
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "norm_phrase": {
                            "query": "",
                            "fuzziness": 1,
                        },
                    }
                }
            }
        },
        "size": 5,
        "sort": {
            "_score": {
                "order": "desc"
            }
        },
    }
es = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
with open("sort.txt", "w") as file:
	#for i_file, fileName in enumerate(os.listdir(path)):
		#if i_file % 50 == 0:
			#print("files processed:", i_file)
		with open("Diseasename.txt", "r") as f:
			for i_line, line in enumerate(f):
				#line = line.split("\t")
				#for line in line:
				norm_phrase = line
				BIGRAM_QUERY_TEMPLATE["query"]["bool"]["must"]["match"]["norm_phrase.bigrams"]["query"] = norm_phrase
				res = es.search(index='en_meddra', body=BIGRAM_QUERY_TEMPLATE)

				if len(res["hits"]["hits"])>0:
					maxHit = sorted(res["hits"]["hits"], key=lambda x: x["_score"])[-1]
				else:
					maxHit = {"_source":{"norm_phrase": "None"}}
				file.write(json.dumps(maxHit["_source"]["norm_phrase"])) 
					

				file.write("\n")